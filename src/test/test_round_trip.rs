use mio::{Poll, Token, Events, PollOpt, Ready};
use std::io;
use std::time::{Duration, Instant};
use std::fs::File;
use std::io::Write;
use ::util;
use ::UtpSocket;

#[test]
fn round_trip() {
    let _ = ::env_logger::init();
    ::util::reset_rand();

    single_round_trip(1, 0.0, false, Duration::from_millis(100));
    single_round_trip(10, 0.0, false, Duration::from_millis(100));
    single_round_trip(100, 0.0, false, Duration::from_millis(100));
    single_round_trip(1_000, 0.0, false, Duration::from_millis(100));
    single_round_trip(1_000_000, 0.0, false, Duration::new(10, 0));

    // test a disconnect
    single_round_trip(1_000_000, 0.0, true, Duration::new(10, 0));

    // test with packet loss
    //single_round_trip(1_000_000, 0.8, false, Duration::new(3600, 0));
}

fn single_round_trip(num_bytes: usize, loss_rate: f32, testing_disconnect: bool, time_limit: Duration) {
    let before = Instant::now();
    let disconnect_timeout = Duration::new(5, 0);
    let mut disconnect_time: Option<Instant> = None;

    let poll = Poll::new().unwrap();
    let rw = Ready::readable() | Ready::writable();
    let (socket_a, listener_a) = UtpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    poll.register(&socket_a, Token(0), rw, PollOpt::edge()).unwrap();;
    poll.register(&listener_a, Token(1), rw, PollOpt::edge()).unwrap();
    let (socket_b, listener_b) = UtpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    poll.register(&socket_b, Token(2), rw, PollOpt::edge()).unwrap();
    poll.register(&listener_b, Token(3), rw, PollOpt::edge()).unwrap();

    let addr = socket_b.local_addr().unwrap();
    let stream_a = socket_a.connect(&addr).unwrap();
    stream_a.set_loss_rate(loss_rate);
    poll.register(&stream_a, Token(4), rw, PollOpt::edge()).unwrap();
    let mut stream_b = None;

    let send_buf: Vec<u8> = (0..num_bytes).into_iter().map(|_| util::rand()).collect();
    let mut recv_buf: Vec<u8> = (0..num_bytes).into_iter().map(|_| 0).collect();
    let mut middle_buf: Vec<u8> = (0..num_bytes).into_iter().map(|_| 1).collect();
    let mut bytes_sent_a = 0;
    let mut bytes_recv_a = 0;
    let mut bytes_sent_b = 0;
    let mut bytes_recv_b = 0;
    let mut bytes_to_check = num_bytes;
    let mut disconnected_a = false;
    let mut disconnected_b = false;

    let tick_duration = Duration::from_millis(500);
    let mut next_tick = Instant::now() + tick_duration;
    'big_loop: loop {
        let now = Instant::now();
        if now > before + time_limit {
            panic!("Timed out!");
        }
        let duration = loop {
            if now > next_tick {
                socket_a.tick().unwrap();
                socket_b.tick().unwrap();
                next_tick += tick_duration;
            } else {
                break next_tick - now;
            }
        };

        let mut events = Events::with_capacity(4);
        poll.poll(&mut events, Some(duration)).unwrap();
        for event in &events {
            match event.token() {
                Token(0) => {
                    let _ = socket_a.ready(event.readiness()).unwrap();
                },
                Token(1) => (),
                Token(2) => {
                    let _ = socket_b.ready(event.readiness()).unwrap();
                },
                Token(3) => {
                    match listener_b.accept() {
                        Ok(stream) => {
                            stream.set_loss_rate(loss_rate);
                            poll.register(&stream, Token(5), rw, PollOpt::edge()).unwrap();
                            assert!(stream_b.is_none());
                            stream_b = Some(stream);
                        },
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (),
                        Err(e) => panic!("error: {:?}", e),
                    }
                },
                Token(4) => {
                    if event.readiness().is_readable() {
                        match stream_a.read(&mut recv_buf[bytes_recv_a..]) {
                            Ok(n) => {
                                if recv_buf[bytes_recv_a..bytes_recv_a + n] != middle_buf[bytes_recv_a..bytes_recv_a + n] {
                                    panic!("(a) Data corrupted! {} .. {}", bytes_recv_a, bytes_recv_a + n);
                                }
                                bytes_recv_a += n;
                                if bytes_recv_a == num_bytes {
                                    break 'big_loop;
                                }
                            },
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (),
                            Err(e) => {
                                if e.kind() == io::ErrorKind::ConnectionReset {
                                    let now = Instant::now();
                                    let target = disconnect_time.unwrap() + disconnect_timeout;
                                    let diff = if now > target { now - target } else { target - now };
                                    assert!(diff < Duration::from_millis(600));
                                    disconnected_a = true;
                                    bytes_to_check = bytes_recv_a;
                                    if disconnected_a && disconnected_b {
                                        break 'big_loop;
                                    }
                                } else {
                                    panic!("error: {:?}", e);
                                }
                            },
                        };
                    }
                    if event.readiness().is_writable() && bytes_sent_a < num_bytes {
                        match stream_a.write(&send_buf[bytes_sent_a..]) {
                            Ok(n) => {
                                bytes_sent_a += n;
                            },
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (),
                            Err(e) => panic!("error: {:?}", e),
                        };
                        if let Some(ref stream) = stream_b {
                            if testing_disconnect && bytes_sent_a >= num_bytes / 2 && disconnect_time.is_none() {
                                stream_a.set_loss_rate(1.0);
                                stream_a.set_disconnect_timeout(disconnect_timeout);
                                stream.set_loss_rate(1.0);
                                stream.set_disconnect_timeout(disconnect_timeout);
                                disconnect_time = Some(Instant::now());
                            }
                        }
                    }
                },
                Token(5) => {
                    let stream_b = stream_b.as_ref().unwrap();
                    if event.readiness().is_readable() {
                        match stream_b.read(&mut middle_buf[bytes_recv_b..]) {
                            Ok(n) => {
                                if middle_buf[bytes_recv_b..bytes_recv_b + n] != send_buf[bytes_recv_b..bytes_recv_b + n] {
                                    panic!("(b) Data corrupted! {} .. {}", bytes_recv_a, bytes_recv_a + n);
                                }
                                bytes_recv_b += n;
                            },
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (),
                            Err(e) => {
                                if e.kind() == io::ErrorKind::ConnectionReset {
                                    let now = Instant::now();
                                    let target = disconnect_time.unwrap() + disconnect_timeout;
                                    let diff = if now > target { now - target } else { target - now };
                                    assert!(diff < Duration::from_millis(600));
                                    disconnected_b = true;
                                    if disconnected_a && disconnected_b {
                                        break 'big_loop;
                                    }
                                } else {
                                    panic!("error: {:?}", e);
                                }
                            },
                        };
                    }
                    if bytes_recv_b - bytes_sent_b > 0 {
                        match stream_b.write(&middle_buf[bytes_sent_b..bytes_recv_b]) {
                            Ok(n) => {
                                bytes_sent_b += n;
                            },
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (),
                            Err(e) => panic!("error: {:?}", e),
                        };
                    }
                },
                t => panic!("whaa? {:?}", t),
            }
        }
    }

    let send_buf = &send_buf[..bytes_to_check];
    let recv_buf = &recv_buf[..bytes_to_check];
    let middle_buf = &middle_buf[..bytes_to_check];

    if send_buf != recv_buf {
        let mut send = File::create("mio-utp-round-trip-failed-send.dat").unwrap();
        let mut recv = File::create("mio-utp-round-trip-failed-recv.dat").unwrap();
        let mut middle = File::create("mio-utp-round-trip-failed-middle.dat").unwrap();
        send.write_all(&send_buf).unwrap();
        recv.write_all(&recv_buf).unwrap();
        middle.write_all(&middle_buf).unwrap();
        panic!("Data corrupted during round-trip! \
               Sent/intermediate/received data saved to mio-utp-round-trip-failed-send.dat, \
               mio-utp-round-trip-middle.dat and mio-utp-round-trip-failed-recv.dat");
    }

    let after = Instant::now();
    let duration = after - before;
    info!("time taken == {}s + {}ns", duration.as_secs(), duration.subsec_nanos());
}

