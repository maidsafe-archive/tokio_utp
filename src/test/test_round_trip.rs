use mio::{Poll, Token, Events, PollOpt, Ready};
use std::io;
use std::time::{Duration, Instant};
use std::sync::mpsc;
use std::thread;
use std::fs::File;
use std::io::Write;
use std::cmp;
use ::util;
use ::UtpSocket;

enum Explode {}

#[test]
fn round_trip() {
    let _ = ::env_logger::init();
    ::util::reset_rand();

    let mut amounts = Vec::new();
    for i in 0..20 {
        let rough_amount = 1 << i;
        for j in 0..cmp::min(cmp::max(1, rough_amount >> 4), 100) {
            let num_bytes = rough_amount + (util::rand::<usize>() % rough_amount);
            amounts.push(num_bytes);
        }
    }

    let num_tests = amounts.len();
    for (i, num_bytes) in amounts.into_iter().enumerate() {
        info!("test {} of {}, sending {} bytes", i + 1, num_tests, num_bytes);
        single_round_trip(num_bytes);
    }
}

fn single_round_trip(num_bytes: usize) {
    let (tx, rx) = mpsc::channel();
    let _joiner = thread::spawn(move || {
        single_round_trip_inner(num_bytes);
        tx.send(()).unwrap();
    });
    let nanos = 1_000_000 + 10_000 * num_bytes as u64;
    rx.recv_timeout(Duration::new(nanos / 1000_000_000, (nanos % 1000_000_000) as u32)).unwrap();
}

fn single_round_trip_inner(num_bytes: usize) {
    let before = Instant::now();
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

    let send_buf: Vec<u8> = (0..num_bytes).into_iter().map(|_| util::rand()).collect();
    let mut recv_buf: Vec<u8> = (0..num_bytes).into_iter().map(|_| 0).collect();
    let mut middle_buf: Vec<u8> = (0..num_bytes).into_iter().map(|_| 1).collect();
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
                                //println!("copied {}/{} bytes", bytes_recv_a, num_bytes);
                                if bytes_recv_a == num_bytes {
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
}

