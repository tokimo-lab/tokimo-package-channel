use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=proto/pbbp2.proto");

    if std::env::var_os("PROTOC").is_none() {
        if let Ok(p) = protoc_bin_vendored::protoc_bin_path() {
            // SAFETY: build.rs is single-threaded.
            unsafe { std::env::set_var("PROTOC", p) };
        }
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_protos(&["proto/pbbp2.proto"], &["proto/"])
        .expect("compile pbbp2.proto");
}
