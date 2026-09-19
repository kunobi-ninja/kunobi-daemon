//! Codec CPU time only; batch means are not socket-latency percentiles.
use buffa::Message;
use kunobi_daemon::wire::{Control, MessageKind, operation};
use std::{hint::black_box, time::Instant};

fn sample(mut operation: impl FnMut()) -> (u128, u128) {
    const ITERATIONS: u32 = 10_000;
    for _ in 0..ITERATIONS {
        operation();
    }
    let mut samples = Vec::new();
    for _ in 0..31 {
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            operation();
        }
        samples.push(start.elapsed().as_nanos() / u128::from(ITERATIONS));
    }
    samples.sort_unstable();
    (samples[15], samples[29])
}

fn main() {
    println!("codec,message,operation,p50_batch_mean_ns,p95_batch_mean_ns,body_bytes");
    let messages = [
        (
            "health",
            Control {
                operation: operation::HEALTH,
                request_id: 42,
                ..Default::default()
            },
        ),
        (
            "receipt",
            Control {
                operation: operation::READY,
                generation: 42,
                token: b"0123456789abcdef0123456789abcdef".to_vec(),
                offset: Some(1_048_576),
                ..Default::default()
            },
        ),
        (
            "application-4k",
            Control {
                kind: MessageKind::Application.into(),
                operation: 1,
                payload: vec![42; 4096],
                ..Default::default()
            },
        ),
    ];
    for (name, message) in messages {
        let encoded = message.encode_to_vec();
        assert_eq!(Control::decode_from_slice(&encoded).unwrap(), message);
        let mut scratch = Vec::with_capacity(encoded.len());
        let mut cache = buffa::SizeCache::new();
        let (p50, p95) = sample(|| {
            scratch.clear();
            black_box(&message)
                .try_encode_bounded_with_cache(65_536, &mut cache, black_box(&mut scratch))
                .unwrap();
            black_box(&scratch);
        });
        println!("protobuf-buffa,{name},encode,{p50},{p95},{}", encoded.len());
        let options = buffa::DecodeOptions::new()
            .with_max_message_size(65_536)
            .with_recursion_limit(32)
            .with_unknown_field_limit(128)
            .with_element_memory_limit(65_536);
        let (p50, p95) = sample(|| {
            black_box(
                options
                    .decode_from_slice::<Control>(black_box(&encoded))
                    .unwrap(),
            );
        });
        println!("protobuf-buffa,{name},decode,{p50},{p95},{}", encoded.len());
    }
}
