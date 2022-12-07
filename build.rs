use std::env;
use std::path::Path;
use std::path::PathBuf;

fn main() {
    let manifest_dir =
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR env variable unset");

    let proto_dir = Path::new(&manifest_dir).join("proto");
    let protos = [
        &Path::new(&proto_dir).join(Path::new("eraftpb.proto")),
        &Path::new(&proto_dir).join(Path::new("multiraftpb.proto")),
    ];

    for proto in protos.iter() {
        println!("cargo:rerun-if-changed={}", proto.to_str().unwrap());
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let mut build_config = prost_build::Config::new();
    build_config
    .file_descriptor_set_path(out_dir.join("pirate_descriptor.bin"))
    .compile_protos(&protos, &[proto_dir]).unwrap();
}
