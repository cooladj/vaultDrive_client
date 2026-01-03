use std::io::Result;
use std::path::Path;
use std::fs;

fn main() -> Result<()> {
    #[cfg(target_os = "windows")]
    {
        // Enable delay-loading for WinFSP DLL
        // This is the officially supported method by WinFSP
        winfsp::build::winfsp_link_delayload();

        println!("cargo:rerun-if-changed=build.rs");
    }

    // Ensure output directory exists
    let out_dir = "src/proto";
    if !Path::new(out_dir).exists() {
        fs::create_dir_all(out_dir)?;
    }

    // Create config and add bincode derives
    let mut config = prost_build::Config::new();
    // config.type_attribute(".", "#[derive(bincode::Encode, bincode::Decode)]");

    // Compile protocol buffers
    match config
        .out_dir(out_dir)
        .compile_protos(
            &["proto/vaultdrive.proto", "proto/hub.proto"],
            &["proto/"],
        ) {
        Ok(_) => {
            println!("cargo:warning=Successfully compiled protocol buffers");
        }
        Err(e) => {
            println!("cargo:warning=Failed to compile protocol buffers: {}", e);
            println!("cargo:warning=Please install protobuf compiler (protoc)");
            println!("cargo:warning=Continuing with potentially stale generated files...");

            // Create placeholder files if they don't exist
            let vaultdrive_rs = Path::new(out_dir).join("vaultdrive.rs");
            let hub_rs = Path::new(out_dir).join("hub.rs");

            if !vaultdrive_rs.exists() {
                fs::write(&vaultdrive_rs, "// Protocol buffer stubs - install protoc and rebuild\n")?;
            }
            if !hub_rs.exists() {
                fs::write(&hub_rs, "// Protocol buffer stubs - install protoc and rebuild\n")?;
            }
        }
    }

    println!("cargo:rerun-if-changed=proto/vaultdrive.proto");
    println!("cargo:rerun-if-changed=proto/hub.proto");
    println!("cargo:rerun-if-changed=build.rs");

    Ok(())
}