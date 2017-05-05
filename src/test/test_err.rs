use super::prelude::*;

#[test]
fn remote_reset() {
    const CONNECTION_ID: u16 = 25103;

    let _ = ::env_logger::init();
    ::util::reset_rand();

    let (socket, _) = Harness::new();
    let mock = Mock::new();
    let server = mock.local_addr();

    let addr = socket.local_addr();
    let th = mock.background(move |m| {
        // Receive the SYN packet
        let p = m.recv_from(&addr);

        assert_eq!(p.ty(), packet::Type::Syn);

        // Send the state packet representing the connection ACK
        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(1);

        // Send the STATE packet
        m.send_to(p, &addr);

        // Send RESET
        let mut p = Packet::reset();
        p.set_connection_id(CONNECTION_ID);
        m.send_to(p, &addr);

        m.assert_quiescence(100);
    });

    let stream = socket.connect(server);

    // The socket becomes writable
    socket.wait_until(|| stream.is_readable());

    let mut buf = [0; 128];
    assert!(stream.read(&mut buf).is_err());

    th.join().unwrap();
}

#[test]
fn remote_sends_invalid_packet() {
    const CONNECTION_ID: u16 = 25103;

    let _ = ::env_logger::init();
    ::util::reset_rand();

    let (socket, _) = Harness::new();
    let mock = Mock::new();
    let server = mock.local_addr();

    let addr = socket.local_addr();
    let th = mock.background(move |m| {
        // Receive the SYN packet
        let p = m.recv_from(&addr);

        assert_eq!(p.ty(), packet::Type::Syn);

        // Send the state packet representing the connection ACK
        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(1);

        // Send the STATE packet
        m.send_to(p, &addr);

        // Send an invalid packet
        let mut p = Packet::state();
        p.set_connection_id(12345);
        m.send_to(p, &addr);

        // Receive the RESET packet
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Reset);
        assert_eq!(p.connection_id(), 12345);
    });

    let stream = socket.connect(server);

    // The socket becomes writable
    socket.tick_for(100);

    th.join().unwrap();
}
