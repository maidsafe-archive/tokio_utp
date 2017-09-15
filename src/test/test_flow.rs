use super::prelude::*;

#[test]
fn ramp_up() {
    const CONNECTION_ID: u16 = 25103;

    let _ = ::env_logger::init();
    ::util::reset_rand();

    let (socket, _) = Harness::new();
    let mock = Mock::new();
    let server = mock.local_addr();

    let addr = socket.local_addr();
    let th = mock.background(move |m| {
        let t = Time::new();

        // Receive the SYN packet
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Syn);
        let ts1 = p.timestamp();
        let ts2 = t.timestamp();

        // Send the state packet representing the connection ACK
        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(1);
        p.set_timestamp(ts2);
        p.set_timestamp_diff(ts2.wrapping_sub(ts1));

        // Send the STATE packet
        m.send_to(p, &addr);

        // Receive start of the data
        let p = m.recv_from(&addr);
        assert_eq!(p.ty(), packet::Type::Data);
        assert_eq!(p.payload().len(), 1374);
        let ts1 = p.timestamp();
        let ts2 = t.timestamp();

        // Send the state packet representing the connection ACK
        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(2);
        p.set_timestamp(ts2);
        p.set_timestamp_diff(ts2.wrapping_sub(ts1));
        m.send_to(p, &addr);

        let mut total = 1374;
        let mut ts1 = 0;

        for _ in 0..4 {
            let p = m.recv_from(&addr);
            ts1 = p.timestamp();
            assert_eq!(p.ty(), packet::Type::Data);
            total += p.len();
        }
        assert_eq!(total, 5694);

        let ts2 = t.timestamp();

        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(6);
        p.set_timestamp(ts2);
        p.set_timestamp_diff(ts2.wrapping_sub(ts1));
        m.send_to(p, &addr);

        for _ in 0..6 {
            let p = m.recv_from(&addr);
            ts1 = p.timestamp();
            assert_eq!(p.ty(), packet::Type::Data);
            total += p.len();
        }
        assert_eq!(total, 12930);

        let ts2 = t.timestamp();

        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(12);
        p.set_timestamp(ts2);
        p.set_timestamp_diff(ts2.wrapping_sub(ts1));
        m.send_to(p, &addr);

        for _ in 0..8 {
            let p = m.recv_from(&addr);
            ts1 = p.timestamp();
            assert_eq!(p.ty(), packet::Type::Data);
            total += p.len();
        }
        assert_eq!(total, 23089);

        sleep(200);
        let ts2 = t.timestamp();

        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(20);
        p.set_timestamp(ts2);
        p.set_timestamp_diff(ts2.wrapping_sub(ts1));

        sleep(200);

        m.send_to(p, &addr);

        for _ in 0..9 {
            let p = m.recv_from(&addr);
            ts1 = p.timestamp();
            assert_eq!(p.ty(), packet::Type::Data);
            total += p.len();
        }
        assert_eq!(total, 34491);

        sleep(200);
        let ts2 = t.timestamp();

        let mut p = Packet::state();
        p.set_connection_id(CONNECTION_ID);
        p.set_seq_nr(123);
        p.set_ack_nr(29);
        p.set_timestamp(ts2);
        p.set_timestamp_diff(ts2.wrapping_sub(ts1));

        sleep(200);

        m.send_to(p, &addr);
    });

    let stream = socket.connect(server);

    // The socket becomes writable
    socket.wait_until(|| stream.is_writable());

    let mut buf = vec![1; 36069];

    while !buf.is_empty() {
        assert!(stream.is_writable());
        // Write some data
        let n = stream.write(&buf).unwrap();
        buf.drain(..n);
        socket.wait_until(|| stream.is_writable());
        // assert_eq!(n, 10_000);
    }

    // Tick a bunch
    socket.tick_for(1500);

    th.join().unwrap();

    drop(stream);
}
