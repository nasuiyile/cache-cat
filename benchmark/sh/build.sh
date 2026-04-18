# windows
$env:RUSTFLAGS="--cfg tokio_unstable"
cargo zigbuild --target x86_64-unknown-linux-gnu --release