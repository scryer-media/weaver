// Every shape that can only hold addresses passes.

use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, TcpListener, TcpStream,
    ToSocketAddrs, UdpSocket,
};

const LOOPBACK: &str = "127.0.0.1:0";

fn main() {
    let v4 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 80);
    let v6 = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 80, 0, 0);
    let any = SocketAddr::V4(v4);

    let _ = TcpStream::connect(any);
    let _ = TcpStream::connect(v4);
    let _ = TcpStream::connect(v6);
    let _ = TcpStream::connect((IpAddr::V4(Ipv4Addr::LOCALHOST), 80));
    let _ = TcpStream::connect((Ipv4Addr::LOCALHOST, 80));
    let _ = TcpStream::connect((Ipv6Addr::LOCALHOST, 80));
    let _ = TcpStream::connect(&[any, SocketAddr::V6(v6)][..]);
    let _ = TcpStream::connect(&any);

    // Literal addresses never reach the resolver.
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let _ = socket.send_to(b"ping", "[::1]:6969");
    let _ = socket.connect(("127.0.0.1", 6969));
    let _ = TcpListener::bind(LOOPBACK);
    let _ = TcpListener::bind(concat!("127.0.0.1", ":", "0"));

    let _ = any.to_socket_addrs();

    let _: Vec<_> = std::iter::once(any).map(TcpStream::connect).collect();
}
