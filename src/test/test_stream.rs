use super::prelude::*;
use std::io;

#[test]
fn connect_echo_close() {
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

        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(1);

        // Send the STATE packet
        m.send_to(p, &addr);

        // No further packets sent on the socket
        m.assert_quiescence(1_000);
    });

    let stream = socket.connect(server);

    // The socket becomes writable
    socket.wait_until(|| stream.is_writable());

    // The socket should not be readable
    assert!(!stream.is_readable());

    // Wait for the server half
    let mock = th.join().unwrap();

    // Write some data
    let n = stream.write(b"hello world").unwrap();
    assert_eq!(n, 11);

    // Receive the data
    let addr = socket.local_addr();
    let th = mock.background(move |m| {
        // Receive the data packet
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Data);
        assert_eq!(p.payload(), b"hello world");
        assert_eq!(p.seq_nr(), 2);
        assert_eq!(p.ack_nr(), 123);

        // Send back the state packet
        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123); // Don't inc seq nr
        p.set_ack_nr(2);

        m.send_to(p, &addr);

        // No further packets sent on the socket
        m.assert_quiescence(500);

        // Send a packet back
        let mut p = Packet::data(b"this is my reply");
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(124);
        p.set_ack_nr(2);

        m.send_to(p, &addr);
        // Receive the ACK
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::State);
        assert_eq!(p.seq_nr(), 2);
        assert_eq!(p.ack_nr(), 124);
    });

    // The socket becomes writable
    socket.wait_until(|| stream.is_readable());

    let mock = th.join().unwrap();

    // Read the data out of the stream buffer
    let mut buf = [0; 128];
    assert_eq!(16, stream.read(&mut buf).unwrap());
    assert_eq!(&buf[..16], b"this is my reply");

    // Dropping the stream will send a FIN
    drop(stream);

    // Let the socket process the FIN
    socket.tick_for(200);

    let addr = socket.local_addr();
    let th = mock.background(move |m| {
        // Receive the FIN packet
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Fin);
        assert_eq!(p.seq_nr(), 3);
        assert_eq!(p.ack_nr(), 124);
    });

    th.join().unwrap();
}

#[test]
fn connect_out_of_order_recv() {
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

        // Send back data packet...
        let mut p = Packet::data(b"this is my msg");
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(124);
        p.set_ack_nr(1);

        // Send the STATE packet
        m.send_to(p, &addr);

        // No further packets sent on the socket
        m.assert_quiescence(100);

        // Send the state packet representing the connection ACK
        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(1);

        // Send the STATE packet
        m.send_to(p, &addr);

        // Receive the ACK for the initial data
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::State);
        assert_eq!(p.seq_nr(), 1);
        assert_eq!(p.ack_nr(), 124);

        // Receive the FIN packet
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Fin);
        assert_eq!(p.seq_nr(), 2);
        assert_eq!(p.ack_nr(), 124);
    });

    let stream = socket.connect(server);

    // The socket becomes writable
    socket.wait_until(|| stream.is_readable());

    // Read the data out of the stream buffer
    let mut buf = [0; 128];
    assert_eq!(14, stream.read(&mut buf).unwrap());
    assert_eq!(&buf[..14], b"this is my msg");

    // Dropping the stream will send a FIN
    drop(stream);

    // Let the socket process the FIN
    socket.tick_for(200);

    th.join().unwrap();
}

#[test]
fn ignores_dup_packets() {
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

        // Send back data packet...
        let mut p = Packet::data(b"this is my msg");
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(124);
        p.set_ack_nr(1);

        // Send the STATE packet
        m.send_to(p.clone(), &addr);

        // Dup packet
        m.send_to(p, &addr);

        // Receive the ACK for the initial data
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::State);
        assert_eq!(p.seq_nr(), 1);
        assert_eq!(p.ack_nr(), 124);

        // Receive the FIN packet
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Fin);
        assert_eq!(p.seq_nr(), 2);
        assert_eq!(p.ack_nr(), 124);
    });

    let stream = socket.connect(server);

    // The socket becomes writable
    socket.wait_until(|| stream.is_readable());

    // Read the data out of the stream buffer
    let mut buf = [0; 128];
    assert_eq!(14, stream.read(&mut buf).unwrap());
    assert_eq!(&buf[..14], b"this is my msg");

    // Wait for more data... potentially
    socket.tick_for(200);

    assert_eq!(io::ErrorKind::WouldBlock,
               stream.read(&mut buf).unwrap_err().kind());

    // Dropping the stream will send a FIN
    drop(stream);

    // Let the socket process the FIN
    socket.tick_for(200);

    th.join().unwrap();
}

#[test]
fn remote_close() {
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

        // Send FIN
        let mut p = Packet::fin();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(124);
        p.set_ack_nr(1);
        m.send_to(p, &addr);

        // Receive the FIN packet
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Fin);
        assert_eq!(p.seq_nr(), 2);
        assert_eq!(p.ack_nr(), 124);

        // Ack fin
        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(124);
        p.set_ack_nr(2);
        m.send_to(p, &addr);
    });

    let stream = socket.connect(server);

    // The socket becomes writable
    socket.wait_until(|| stream.is_readable());

    // Wait a bit more...
    socket.tick_for(200);

    let mut buf = [0; 128];
    assert_eq!(0, stream.read(&mut buf).unwrap());

    drop(stream);

    th.join().unwrap();
}
