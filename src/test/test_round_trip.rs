use mio::{Poll, Token, Events, PollOpt, Ready};
use std::io;
use std::time::{Duration, Instant};
use std::sync::mpsc;
use std::thread;
use ::util;
use ::UtpSocket;

enum Explode {}

#[test]
fn round_trip() {
    let (tx, rx) = mpsc::channel();
    let joiner = thread::spawn(move || {
        round_trip_inner();
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(60)).unwrap();
}

fn round_trip_inner() {
    const NUM_BYTES: usize = 1024 * 1024;

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
    poll.register(&stream_a, Token(4), rw, PollOpt::edge()).unwrap();
    let mut stream_b = None;

    let send_buf: Vec<u8> = (0..NUM_BYTES).into_iter().map(|_| util::rand()).collect();
    let mut recv_buf: Vec<u8> = (0..NUM_BYTES).into_iter().map(|_| 0).collect();
    let mut middle_buf: Vec<u8> = (0..NUM_BYTES).into_iter().map(|_| 1).collect();
    let mut bytes_sent_a = 0;
    let mut bytes_recv_a = 0;
    let mut bytes_sent_b = 0;
    let mut bytes_recv_b = 0;

    let tick_duration = Duration::from_millis(500);
    let mut next_tick = Instant::now() + tick_duration;
    'big_loop: loop {
        let now = Instant::now();
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
                Token(0) => socket_a.ready(event.readiness()).unwrap(),
                Token(1) => (),
                Token(2) => socket_b.ready(event.readiness()).unwrap(),
                Token(3) => {
                    match listener_b.accept() {
                        Ok(stream) => {
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
                                bytes_recv_a += n;
                                println!("copied {}/{} bytes", bytes_recv_a, NUM_BYTES);
                                if bytes_recv_a == NUM_BYTES {
                                    break 'big_loop;
                                }
                            },
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (),
                            Err(e) => panic!("error: {:?}", e),
                        };
                    }
                    if event.readiness().is_writable() {
                        match stream_a.write(&send_buf[bytes_sent_a..]) {
                            Ok(n) => bytes_sent_a += n,
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (),
                            Err(e) => panic!("error: {:?}", e),
                        };
                    }
                },
                Token(5) => {
                    let stream_b = stream_b.as_ref().unwrap();
                    if event.readiness().is_readable() {
                        match stream_b.read(&mut middle_buf[bytes_recv_b..]) {
                            Ok(n) => bytes_recv_b += n,
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (),
                            Err(e) => panic!("error: {:?}", e),
                        };
                    }
                    match stream_b.write(&middle_buf[bytes_sent_b..bytes_recv_b]) {
                        Ok(n) => bytes_sent_b += n,
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => (),
                        Err(e) => panic!("error: {:?}", e),
                    };
                },
                t => panic!("whaa? {:?}", t),
            }
        }
    }

    assert_eq!(bytes_sent_a, NUM_BYTES);
    assert_eq!(bytes_recv_a, NUM_BYTES);
    assert_eq!(bytes_sent_b, NUM_BYTES);
    assert_eq!(bytes_recv_b, NUM_BYTES);
    assert_eq!(send_buf, recv_buf);
}

