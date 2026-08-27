document.addEventListener('DOMContentLoaded', function () {
    window.setTimeout(()=>{
        document.getElementById("link").click();
    },1000);
    //window.open("xdm-app:chrome-extension://" + chrome.runtime.id + "/");
    // chrome.runtime.getURL() returns the right scheme on both browsers
    // (chrome-extension:// vs moz-extension://), unlike hardcoding one.
    document.getElementById("link").href = "xdm-app:" + chrome.runtime.getURL("/");
}, false);