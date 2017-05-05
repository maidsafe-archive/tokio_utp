use super::prelude::*;

#[test]
fn accept_stream() {
    const CONNECTION_ID: u16 = 25103;

    let _ = ::env_logger::init();
    ::util::reset_rand();

    let (socket, listener) = Harness::new();
    let mock = Mock::new();
    let server = mock.local_addr();

    let addr = socket.local_addr();
    let th = mock.background(move |m| {
        let mut p = Packet::syn();
        p.set_seq_nr(1);
        p.set_connection_id(123);
        m.send_to(p, &addr);

        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::State);
        assert_eq!(p.seq_nr(), 25103);
        assert_eq!(p.ack_nr(), 1);
        assert_eq!(p.connection_id(), 123);

        let mut p = Packet::data(b"this is my message");
        p.set_connection_id(124);
        p.set_seq_nr(2);
        p.set_ack_nr(25103);
        m.send_to(p, &addr);

        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::State);
        assert_eq!(p.seq_nr(), 25103);
        assert_eq!(p.ack_nr(), 2);
        assert_eq!(p.connection_id(), 123);
    });

    // The socket becomes writable
    socket.wait_until(|| listener.is_readable());
    let stream = listener.accept().unwrap();

    socket.wait_until(|| stream.is_readable());

    // Read the data out of the stream buffer
    let mut buf = [0; 128];
    assert_eq!(18, stream.read(&mut buf).unwrap());
    assert_eq!(&buf[..18], b"this is my message");

    th.join().unwrap();
}

#[test]
fn dropping_listener() {
    const CONNECTION_ID: u16 = 25103;

    let _ = ::env_logger::init();
    ::util::reset_rand();

    let (socket, _) = Harness::new();
    let mock = Mock::new();
    let server = mock.local_addr();

    let addr = socket.local_addr();
    let th = mock.background(move |m| {
        let mut p = Packet::syn();
        p.set_seq_nr(1);
        p.set_connection_id(123);
        m.send_to(p, &addr);

        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Reset);
        assert_eq!(p.connection_id(), 123);
    });

    socket.tick_for(200);

    th.join().unwrap();
}
