use super::*;

#[tokio::test]
async fn idle_inspection_has_a_hard_input_budget_and_restores_normal_reads() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(&vec![7; 128 * 1024]).await.unwrap();
    });
    let mut route = RouteStream::from(tokio::net::TcpStream::connect(address).await.unwrap());
    server.await.unwrap();
    route.begin_inspection();
    let mut bytes = vec![0; 128 * 1024];
    let mut read = 0;
    async {
        while read < 64 * 1024 {
            match route.try_read(&mut bytes[read..]) {
                Ok(n) => {
                    assert_ne!(n, 0);
                    read += n;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    tokio::task::yield_now().await
                }
                Err(error) => panic!("inspection failed: {error}"),
            }
        }
    }
    .await;
    assert_eq!(read, 64 * 1024);
    assert_eq!(
        route.try_read(&mut bytes[read..]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    route.end_inspection();
    route.read_exact(&mut bytes[read..]).await.unwrap();
    assert!(bytes.iter().all(|byte| *byte == 7));
}
