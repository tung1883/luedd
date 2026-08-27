//! Prints the native-messaging host manifest for `tidm-nmhost` plus the
//! manual registration steps for the current platform. This tool only prints -
//! it never touches the registry or writes into a browser's profile directory,
//! since that's a system-wide/profile-wide change the user should apply
//! themselves (or explicitly ask an agent to run on their behalf).
//!
//! Usage: tidm-gen-manifest <path-to-tidm-nmhost-exe> [chrome-extension-id]

use tidm_ipc::manifest::manifest;

fn main() {
    let mut args = std::env::args().skip(1);
    let exe_path = args.next().unwrap_or_else(|| {
        eprintln!("usage: tidm-gen-manifest <path-to-tidm-nmhost-exe> [chrome-extension-id]");
        std::process::exit(1);
    });
    let extension_id = args.next();

    let m = manifest(&exe_path, extension_id.as_deref(), None);
    let json = serde_json::to_string_pretty(&m).unwrap();

    println!("Native messaging host manifest for {}:\n", tidm_ipc::manifest::HOST_NAME);
    println!("{json}\n");

    #[cfg(windows)]
    {
        println!("To register with Chrome/Edge on Windows, save the JSON above to a file, e.g.");
        println!(r"  C:\Users\<you>\AppData\Local\tidm\{}.json", tidm_ipc::manifest::HOST_NAME);
        println!("then add a registry value pointing at it (run yourself, not run for you):");
        println!(
            r#"  reg add "HKCU\Software\Google\Chrome\NativeMessagingHosts\{}" /ve /t REG_SZ /d "<path-to-json-file>" /f"#,
            tidm_ipc::manifest::HOST_NAME
        );
    }
    #[cfg(not(windows))]
    {
        println!("To register with Chrome on Linux, save the JSON above to:");
        println!(
            "  ~/.config/google-chrome/NativeMessagingHosts/{}.json",
            tidm_ipc::manifest::HOST_NAME
        );
    }
}
