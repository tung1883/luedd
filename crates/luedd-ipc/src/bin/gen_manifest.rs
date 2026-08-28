
use luedd_ipc::manifest::manifest;

fn main() {
    let mut args = std::env::args().skip(1);
    let exe_path = args.next().unwrap_or_else(|| {
        eprintln!("usage: luedd-gen-manifest <path-to-luedd-nmhost-exe> [chrome-extension-id]");
        std::process::exit(1);
    });
    let extension_id = args.next();

    let m = manifest(&exe_path, extension_id.as_deref(), None);
    let json = serde_json::to_string_pretty(&m).unwrap();

    println!("Native messaging host manifest for {}:\n", luedd_ipc::manifest::HOST_NAME);
    println!("{json}\n");

    #[cfg(windows)]
    {
        println!("To register with Chrome/Edge on Windows, save the JSON above to a file, e.g.");
        println!(r"  C:\Users\<you>\AppData\Local\luedd\{}.json", luedd_ipc::manifest::HOST_NAME);
        println!("then add a registry value pointing at it (run yourself, not run for you):");
        println!(
            r#"  reg add "HKCU\Software\Google\Chrome\NativeMessagingHosts\{}" /ve /t REG_SZ /d "<path-to-json-file>" /f"#,
            luedd_ipc::manifest::HOST_NAME
        );
    }
    #[cfg(not(windows))]
    {
        println!("To register with Chrome on Linux, save the JSON above to:");
        println!(
            "  ~/.config/google-chrome/NativeMessagingHosts/{}.json",
            luedd_ipc::manifest::HOST_NAME
        );
    }
}
