//! This binary is only used for this crate's tests
#[tokio::main]
async fn main() {
    _ = tokio::spawn(async {
        panic!("here is a panic");
    })
    .await;
}
