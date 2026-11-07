// Every shape that can carry a name to the host resolver is refused.

use std::net::{TcpListener, TcpStream, ToSocketAddrs, UdpSocket};

struct Endpoint;

impl ToSocketAddrs for Endpoint {
    type Iter = std::vec::IntoIter<std::net::SocketAddr>;
    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        Ok(Vec::new().into_iter())
    }
}

fn main() {
    let host = String::from("tracker.example");
    let name: &str = "tracker.example:6969";

    let _ = TcpStream::connect(name);
    let _ = TcpStream::connect(format!("{host}:6969"));
    let _ = TcpStream::connect((name, 6969));
    let _ = TcpStream::connect((host.clone(), 6969));
    let _ = TcpStream::connect("localhost:80");

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    let _ = socket.send_to(b"ping", "tracker.example:6969");
    let _ = socket.connect(("tracker.example", 6969));

    let _ = TcpListener::bind(("localhost", 0));

    let _ = TcpStream::connect(Endpoint);

    let _ = name.to_socket_addrs();
    let _ = ToSocketAddrs::to_socket_addrs(&(host.as_str(), 0));

    let _: Vec<_> = std::iter::once(name).map(TcpStream::connect).collect();
}
