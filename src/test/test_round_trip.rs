use UtpSocket;
use std::fs::File;
use std::io::Write;
use tokio_core::reactor::Core;
use tokio_io;
use tokio_io::AsyncRead;
use futures::future;
use futures::{Future, Stream};
use rand::Rng;
use util;

trait FutureExt: Sized + Future + 'static {
    fn sendless_boxed(self) -> Box<Future<Item=Self::Item, Error=Self::Error> + 'static> {
        Box::new(self)
    }
}

impl<T: Future + 'static> FutureExt for T {}

#[test]
fn send_lots_of_data_one_way() {
    const NUM_TESTS: usize = 100;

    let _ = ::env_logger::init();
    util::reset_rand();

    send_data_one_way(0);
    send_data_one_way(1);
    for i in 0..NUM_TESTS {
        info!("Test {} of {}", i + 1, NUM_TESTS);
        let num_bytes = util::THREAD_RNG.with(|r| r.borrow_mut().gen_range(1024 * 1024, 1024 * 1024 * 20));
        send_data_one_way(num_bytes);
    }
}

#[test]
fn send_lots_of_data_round_trip() {
    const NUM_TESTS: usize = 100;

    let _ = ::env_logger::init();
    util::reset_rand();

    send_data_round_trip(0);
    send_data_round_trip(1);
    for i in 0..NUM_TESTS {
        info!("Test {} of {}", i + 1, NUM_TESTS);
        let num_bytes = util::THREAD_RNG.with(|r| r.borrow_mut().gen_range(1024 * 1024, 1024 * 1024 * 20));
        send_data_round_trip(num_bytes);
    }
}

fn send_data_round_trip(num_bytes: usize) {
    println!("== sending {} bytes", num_bytes);

    let random_send = util::random_vec(num_bytes);
    let addr = addr!("127.0.0.1:0");
    
    let mut core = unwrap!(Core::new());
    let handle = core.handle();
    let res = core.run(future::lazy(|| {
        let (socket_a, _) = unwrap!(UtpSocket::bind(&addr, &handle));
        let (_, listener_b) = unwrap!(UtpSocket::bind(&addr, &handle));
        
        let task0 = socket_a.connect(&unwrap!(listener_b.local_addr())).and_then(|stream_a| {
            let (read_half_a, write_half_a) = stream_a.split();
            let task0 = tokio_io::io::write_all(write_half_a, random_send)
                .and_then(|(write_half_a, random_send)| {
                    trace!("finished writing from (a)");
                    tokio_io::io::shutdown(write_half_a).map(|_| random_send)
                })
                .sendless_boxed();
            let task1 = tokio_io::io::read_to_end(read_half_a, Vec::new()).map(|(_, random_recv)| {
                trace!("finished reading at (a)");
                random_recv
            })
                .sendless_boxed();
            task0.join(task1).sendless_boxed()
        });
        let task1 = listener_b.incoming().into_future().map_err(|(e, _)| e).and_then(|(stream_b, _)| {
            let stream_b = unwrap!(stream_b);
            let (read_half_b, write_half_b) = stream_b.split();
            tokio_io::io::copy(read_half_b, write_half_b)
                .and_then(|(_, _, write_half_b)| {
                    trace!("finished copying at (b)");
                    tokio_io::io::shutdown(write_half_b)
                })
                .sendless_boxed()
        });
        task0.join(task1).map(|((random_send, random_recv), _)| {
            if random_send != random_recv {
                let mut send = unwrap!(File::create("round-trip-failed-send.dat"));
                let mut recv = unwrap!(File::create("round-trip-failed-recv.dat"));
                unwrap!(send.write_all(&random_send));
                unwrap!(recv.write_all(&random_recv));
                panic!("Data corrupted during round-trip! Sent/received data saved to round-trip-failed-send.dat and round-trip-failed-recv.dat");
            }
        }).sendless_boxed()
    }));
    unwrap!(res);
}

fn send_data_one_way(num_bytes: usize) {
    let random_send = util::random_vec(num_bytes);
    let addr = addr!("127.0.0.1:0");
    
    let mut core = unwrap!(Core::new());
    let handle = core.handle();
    let res = core.run(future::lazy(|| {
        let (socket_a, _) = unwrap!(UtpSocket::bind(&addr, &handle));
        let (_, listener_b) = unwrap!(UtpSocket::bind(&addr, &handle));
        
        let task0 = socket_a.connect(&unwrap!(listener_b.local_addr())).and_then(|stream_a| {
            tokio_io::io::write_all(stream_a, random_send)
                .and_then(|(stream_a, random_send)| {
                    tokio_io::io::shutdown(stream_a).map(|_| random_send)
                })
                .sendless_boxed()
        });
        let task1 = listener_b.incoming().into_future().map_err(|(e, _)| e).and_then(|(stream_b, _)| {
            let stream_b = unwrap!(stream_b);
            tokio_io::io::read_to_end(stream_b, Vec::new()).map(|(_, random_recv)| random_recv)
                .sendless_boxed()
        });
        task0.join(task1).map(|(random_send, random_recv)| {
            assert_eq!(random_send, random_recv);
            if random_send != random_recv {
                let mut send = unwrap!(File::create("one-way-failed-send.dat"));
                let mut recv = unwrap!(File::create("one-way-failed-recv.dat"));
                unwrap!(send.write_all(&random_send));
                unwrap!(recv.write_all(&random_recv));
                panic!("Data corrupted during one way transfer! Sent/received data saved to one-way-failed-send.dat and one-way-failed-recv.dat");
            }
        }).sendless_boxed()
    }));
    unwrap!(res);
}

