//! This binary is only used for this crate's tests
#[tokio::main]
#[test_macros::abort_on_panic]
async fn main() {
    _ = tokio::spawn(async {
        panic!("here is a panic");
    })
    .await;
}
