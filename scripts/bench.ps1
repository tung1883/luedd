# Download benchmark driver: baseline build vs new build, N runs each, median table.
#
#   cargo build --release -p luedd-core --example bench_download --no-default-features [--features bench_new]
#   scripts\bench.ps1 -Baseline <old bench_download.exe> -New <new bench_download.exe> -Scenarios s1,s2,s3,s4
#
# S1 throughput/CPU/write-ops   S2 uneven-speed tail   S3 cache on C: -> dest on D:   S4 many small files
param(
    [Parameter(Mandatory = $true)][string]$Baseline,
    [Parameter(Mandatory = $true)][string]$New,
    [string[]]$Scenarios = @("s1", "s2", "s3", "s4"),
    [int]$Runs = 5,
    [string]$WorkDir = (Join-Path $env:TEMP "luedd-bench"),
    [string]$CrossDir = "D:\luedd-bench",
    [string]$OutJson = ""
)
$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force $WorkDir | Out-Null

function Start-Server([string]$exe, [string[]]$serveArgs) {
    $out = Join-Path $WorkDir ("srv-" + [guid]::NewGuid().ToString("N") + ".txt")
    $p = Start-Process -FilePath $exe -ArgumentList (@("serve") + $serveArgs) -RedirectStandardOutput $out -PassThru -WindowStyle Hidden
    for ($i = 0; $i -lt 50; $i++) {
        Start-Sleep -Milliseconds 100
        $t = Get-Content $out -ErrorAction SilentlyContinue | Where-Object { $_ -match '^PORT (\d+)' }
        if ($t) { return @{ Proc = $p; Port = [int]($t -replace 'PORT ', '') } }
    }
    throw "server did not start"
}

function Median($xs) {
    $s = @($xs | Sort-Object)
    if ($s.Count -eq 0) { return $null }
    $m = [math]::Floor($s.Count / 2)
    if ($s.Count % 2) { return $s[$m] } else { return ($s[$m - 1] + $s[$m]) / 2 }
}

# one measured run of one build; returns the parsed JSON line
function Run-One([string]$exe, [string]$tag, [string[]]$clientArgs, [string]$cacheDir) {
    New-Item -ItemType Directory -Force $cacheDir | Out-Null
    $env:LUEDD_DATA_DIR = $cacheDir
    $line = & $exe run @clientArgs | Select-Object -Last 1
    Remove-Item Env:\LUEDD_DATA_DIR -ErrorAction SilentlyContinue
    return ($line | ConvertFrom-Json)
}

$results = @()
function Compare-Builds([string]$name, [string[]]$serveArgs, [scriptblock]$argsFor, [string]$cacheDir, [string[]]$metrics, [int]$runs) {
    $srv = Start-Server $Baseline $serveArgs
    try {
        $rows = @{ base = @(); new = @() }
        for ($i = 0; $i -lt $runs; $i++) {
            # alternate order so thermal / cache drift hits both equally
            $order = if ($i % 2 -eq 0) { @("base", "new") } else { @("new", "base") }
            foreach ($which in $order) {
                $exe = if ($which -eq "base") { $Baseline } else { $New }
                $r = Run-One $exe $which (& $argsFor $srv.Port $which) (Join-Path $cacheDir $which)
                if (-not $r.ok) { Write-Warning "$name/$which run $i FAILED verification" }
                $rows[$which] += $r
            }
        }
        $summary = [ordered]@{ scenario = $name }
        foreach ($m in $metrics) {
            $b = Median ($rows.base | ForEach-Object { $_.$m })
            $n = Median ($rows.new | ForEach-Object { $_.$m })
            $summary[$m] = "{0} -> {1}" -f $b, $n
        }
        $okAll = (($rows.base + $rows.new) | Where-Object { -not $_.ok }).Count -eq 0
        $summary["verified"] = $okAll
        $script:results += [pscustomobject]$summary
        $rows
    }
    finally { Stop-Process -Id $srv.Proc.Id -Force -ErrorAction SilentlyContinue }
}

$GiB = 1073741824
$MiB = 1048576
$cache = Join-Path $WorkDir "cache"

foreach ($s in $Scenarios) {
    switch ($s) {
        "s1" {
            foreach ($c in 4, 8) {
                $null = Compare-Builds "S1 1GiB uncapped, $c conns (C:)" @("--size", $GiB) {
                    param($port, $which) @("--scenario", "big", "--url", "http://127.0.0.1:$port", "--dest", (Join-Path $WorkDir "s1-$which.bin"), "--size", $GiB, "--conns", $c)
                } $cache @("mb_s", "cpu_s", "write_ops", "write_bytes_x", "peak_mb") $Runs
            }
        }
        "s2" {
            $size = 512 * $MiB
            $null = Compare-Builds "S2a 512MiB jitter + slow tail, 4 conns" @("--size", $size, "--rate-mbps", 25, "--jitter", "--slow-tail") {
                param($port, $which) @("--scenario", "big", "--url", "http://127.0.0.1:$port", "--dest", (Join-Path $WorkDir "s2-$which.bin"), "--size", $size, "--conns", 4)
            } $cache @("wall_ms", "mb_s", "cpu_s") $Runs
            $null = Compare-Builds "S2b 512MiB uniform 25MB/s per conn, 4 conns" @("--size", $size, "--rate-mbps", 25) {
                param($port, $which) @("--scenario", "big", "--url", "http://127.0.0.1:$port", "--dest", (Join-Path $WorkDir "s2u-$which.bin"), "--size", $size, "--conns", 4)
            } $cache @("wall_ms", "mb_s", "cpu_s") $Runs
        }
        "s3" {
            New-Item -ItemType Directory -Force $CrossDir | Out-Null
            try {
                $null = Compare-Builds "S3 1GiB cache on C: -> dest on D:" @("--size", $GiB) {
                    param($port, $which) @("--scenario", "big", "--url", "http://127.0.0.1:$port", "--dest", (Join-Path $CrossDir "s3-$which.bin"), "--size", $GiB, "--conns", 4)
                } $cache @("wall_ms", "mb_s", "write_bytes_x", "write_ops") ([math]::Min($Runs, 3))
            }
            finally { Remove-Item $CrossDir -Recurse -Force -ErrorAction SilentlyContinue }
        }
        "s4" {
            # old loop: one at a time, 300 ms pause after each item. New: 4 in parallel, no pause.
            $null = Compare-Builds "S4 100 x 200KB, 40ms latency (old loop vs new loop)" @("--latency-ms", 40) {
                param($port, $which)
                $a = @("--scenario", "many", "--url", "http://127.0.0.1:$port", "--dest", (Join-Path $WorkDir "s4-$which"), "--files", 100)
                if ($which -eq "base") { $a += @("--parallel", 1, "--item-delay-ms", 300) } else { $a += @("--parallel", 4, "--item-delay-ms", 0) }
                $a
            } $cache @("wall_ms", "files_s", "write_ops_per_file", "cpu_s") ([math]::Min($Runs, 3))
        }
    }
}

Write-Host ""
$results | Format-Table -AutoSize -Wrap | Out-String -Width 250 | Write-Host
if ($OutJson) { $results | ConvertTo-Json -Depth 4 | Set-Content $OutJson -Encoding utf8 }
