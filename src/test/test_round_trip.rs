use futures::future;
use futures::{Future, Stream};
use future_utils::FutureExt;
use rand::Rng;
use std::fs::File;
use std::io::Write;
use tokio;
use tokio::runtime::Runtime;
use tokio::io::AsyncRead;
use util;
use UtpSocket;
use void::ResultVoidExt;

#[test]
fn send_lots_of_data_one_way() {
    const NUM_TESTS: usize = 100;

    let _ = ::env_logger::init();
    util::reset_rand();

    send_data_one_way(0);
    send_data_one_way(1);
    for i in 0..NUM_TESTS {
        info!("Test {} of {}", i + 1, NUM_TESTS);
        let num_bytes =
            util::THREAD_RNG.with(|r| r.borrow_mut().gen_range(1024 * 1024, 1024 * 1024 * 20));
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
        let num_bytes =
            util::THREAD_RNG.with(|r| r.borrow_mut().gen_range(1024 * 1024, 1024 * 1024 * 20));
        send_data_round_trip(num_bytes);
    }
}

fn send_data_round_trip(num_bytes: usize) {
    println!("== sending {} bytes", num_bytes);

    let random_send = util::random_vec(num_bytes);
    let addr = addr!("127.0.0.1:0");

    let mut runtime = unwrap!(Runtime::new());
    let res = runtime.block_on(future::lazy(move || {
        let (socket_a, _) = unwrap!(UtpSocket::bind(&addr));
        let (_, listener_b) = unwrap!(UtpSocket::bind(&addr));

        let task0 = socket_a
            .connect(&unwrap!(listener_b.local_addr()))
            .and_then(|stream_a| {
                let (read_half_a, write_half_a) = stream_a.split();
                let task0 = tokio::io::write_all(write_half_a, random_send)
                    .and_then(|(write_half_a, random_send)| {
                        trace!("finished writing from (a)");
                        tokio::io::shutdown(write_half_a).map(|_| random_send)
                    })
                    .into_send_boxed();
                let task1 = tokio::io::read_to_end(read_half_a, Vec::new())
                    .map(|(_, random_recv)| {
                        trace!("finished reading at (a)");
                        random_recv
                    })
                    .into_send_boxed();
                task0.join(task1).into_send_boxed()
            });
        let task1 = listener_b
            .incoming()
            .into_future()
            .map_err(|(e, _)| e)
            .and_then(|(stream_b, _)| {
                let stream_b = unwrap!(stream_b);
                let (read_half_b, write_half_b) = stream_b.split();
                tokio::io::copy(read_half_b, write_half_b)
                    .and_then(|(_, _, write_half_b)| {
                        trace!("finished copying at (b)");
                        tokio::io::shutdown(write_half_b)
                    })
                    .into_send_boxed()
            });
        task0
            .join(task1)
            .map(|((random_send, random_recv), _)| {
                if random_send != random_recv {
                    let mut send = unwrap!(File::create("round-trip-failed-send.dat"));
                    let mut recv = unwrap!(File::create("round-trip-failed-recv.dat"));
                    unwrap!(send.write_all(&random_send));
                    unwrap!(recv.write_all(&random_recv));
                    panic!(
                        "Data corrupted during round-trip! Sent/received data saved to
                           round-trip-failed-send.dat and round-trip-failed-recv.dat"
                    );
                }
            })
            .into_send_boxed()
    }));
    unwrap!(res);
}

fn send_data_one_way(num_bytes: usize) {
    let random_send = util::random_vec(num_bytes);
    let addr = addr!("127.0.0.1:0");

    let mut runtime = unwrap!(Runtime::new());
    let res = runtime.block_on(future::lazy(move || {
        let (socket_a, _) = unwrap!(UtpSocket::bind(&addr));
        let (_, listener_b) = unwrap!(UtpSocket::bind(&addr));

        let task0 = socket_a
            .connect(&unwrap!(listener_b.local_addr()))
            .map_err(|e| panic!("error connecting: {}", e))
            .and_then(|stream_a| {
                tokio::io::write_all(stream_a, random_send)
                    .map_err(|e| panic!("error writing: {}", e))
                    .and_then(|(stream_a, random_send)| {
                        tokio::io::shutdown(stream_a)
                            .map(|_| random_send)
                            .map_err(|e| panic!("error shutting down: {}", e))
                    })
                    .into_send_boxed()
            });
        let task1 = listener_b
            .incoming()
            .into_future()
            .map_err(|(e, _)| panic!("error accepting: {}", e))
            .and_then(|(stream_b, _)| {
                let stream_b = unwrap!(stream_b);
                tokio::io::read_to_end(stream_b, Vec::new())
                    .map_err(|e| panic!("error reading: {}", e))
                    .map(|(_, random_recv)| random_recv)
                    .into_send_boxed()
            });
        task0
            .join(task1)
            .map(|(random_send, random_recv)| {
                assert_eq!(random_send, random_recv);
                if random_send != random_recv {
                    let mut send = unwrap!(File::create("one-way-failed-send.dat"));
                    let mut recv = unwrap!(File::create("one-way-failed-recv.dat"));
                    unwrap!(send.write_all(&random_send));
                    unwrap!(recv.write_all(&random_recv));
                    panic!(
                        "Data corrupted during one way transfer! Sent/received data saved to
                           one-way-failed-send.dat and one-way-failed-recv.dat"
                    );
                }
            })
            .into_send_boxed()
    }));
    res.void_unwrap()
}
