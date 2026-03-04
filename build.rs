#[cfg(feature = "compile")]
mod codegen_impl {
    include!("src/codegen_impl.rs");
}

fn main() {
    #[cfg(feature = "compile")]
    {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
            .expect("CARGO_MANIFEST_DIR should always be set by Cargo");
        let proto_dir = std::path::PathBuf::from(manifest_dir).join("tests/proto");
        let proto_file = proto_dir.join("test.proto");

        println!("cargo:rerun-if-changed={}", proto_file.display());
        codegen_impl::configure()
            .compile_protos(&[proto_file], &[proto_dir])
            .expect("failed to compile test proto");
    }
}
