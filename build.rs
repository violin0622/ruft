// use std::{env, path::PathBuf};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let out_dir = env::var("OUT_DIR")?;

    tonic_build::configure()
        // .file_descriptor_set_path(PathBuf::from(out_dir).join("pet_store_reflection.bin"))
        .include_file("mod.rs")
        .compile(&["raft.proto"], &["proto"])?;
    Ok(())
}
