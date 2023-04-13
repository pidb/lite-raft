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
        &Path::new(&proto_dir).join(Path::new("storepb.proto")),
    ];

    for proto in protos.iter() {
        println!("cargo:rerun-if-changed={}", proto.to_str().unwrap());
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    #[cfg(not(feature = "grpc"))]
    prost_build::Config::new()
        .extern_path(".eraftpb", "::raft::eraftpb")
        .file_descriptor_set_path(out_dir.join("oceanraft_descriptor.bin"))
        .message_attribute(
            "multiraft.StoreData",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .message_attribute(
            "multiraft.MembershipChangData",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .message_attribute(
            "multiraft.SingleMembershipChange",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile_protos(&protos, &[proto_dir])
        .unwrap();

    #[cfg(feature = "grpc")]
    tonic_build::configure()
        .extern_path(".eraftpb", "::raft::eraftpb")
        .file_descriptor_set_path(out_dir.join("oceanraft_descriptor.bin"))
        .message_attribute(
            "multiraft.StoreData",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .message_attribute(
            "multiraft.MembershipChangData",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .message_attribute(
            "multiraft.SingleMembershipChange",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .build_client(true)
        .compile(&protos, &[proto_dir])
        .unwrap();
}
