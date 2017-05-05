use super::prelude::*;

#[test]
fn resends_syn_packet_on_timeout() {
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
        assert_eq!(p.version(), 1);
        assert_eq!(p.seq_nr(), 1);
        assert_eq!(p.ack_nr(), 0);

        m.assert_quiescence(100);

        assert_eq!(p.ty(), packet::Type::Syn);
        assert_eq!(p.version(), 1);
        assert_eq!(p.seq_nr(), 1);
        assert_eq!(p.ack_nr(), 0);

        // Send the state packet representing the connection ACK
        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(1);

        // Send the STATE packet
        m.send_to(p, &addr);

        // Get first data packet, but ignore it.
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Data);
        assert_eq!(p.payload(), b"hello world");
        assert_eq!(p.seq_nr(), 2);
        assert_eq!(p.ack_nr(), 123);

        // ACK the packet
        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(2);
    });

    let stream = socket.connect(server);

    // The socket becomes writable
    socket.wait_until(|| stream.is_writable());

    // Write some data
    let n = stream.write(b"hello world").unwrap();
    assert_eq!(n, 11);

    // Tick a bunch
    socket.tick_for(1500);

    th.join().unwrap();

    drop(stream);
}

#[test]
fn resends_data_packet_on_timeout() {
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

        // Get first data packet, but ignore it.
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Data);
        assert_eq!(p.payload(), b"hello world");
        assert_eq!(p.seq_nr(), 2);
        assert_eq!(p.ack_nr(), 123);

        m.assert_quiescence(100);

        // Get the packet again
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Data);
        assert_eq!(p.payload(), b"hello world");
        assert_eq!(p.seq_nr(), 2);
        assert_eq!(p.ack_nr(), 123);

        // ACK the packet
        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(2);
    });

    let stream = socket.connect(server);

    // The socket becomes writable
    socket.wait_until(|| stream.is_writable());

    // Write some data
    let n = stream.write(b"hello world").unwrap();
    assert_eq!(n, 11);

    // Tick a bunch
    socket.tick_for(1500);

    th.join().unwrap();

    drop(stream);
}
