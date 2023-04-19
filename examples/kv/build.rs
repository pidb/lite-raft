use std::{env, path::PathBuf};

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("oceanraft_kv_example.bin"))
        .build_client(true)
        .build_server(true)
        .compile(&["proto/kv.proto"], &["proto"])
        .unwrap();
}
