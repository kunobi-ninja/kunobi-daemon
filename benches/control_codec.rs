//! Repeated codec-only samples. This does not measure socket or upgrade latency.
use bilrost::{Message, OwnedMessage};
use kunobi_daemon::wire::{Control, operation};
use serde::{Deserialize, Serialize};
use std::{hint::black_box, time::Instant};

#[derive(Serialize, Deserialize)]
struct JsonControl {
    action: String,
    generation: u64,
    token: String,
    offset: u64,
}

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
    let token = "0123456789abcdef0123456789abcdef";
    let binary = Control {
        operation: operation::READY,
        generation: 42,
        token: token.as_bytes().to_vec(),
        offset: Some(1_048_576),
        ..Default::default()
    };
    let json = JsonControl {
        action: "ready".into(),
        generation: 42,
        token: token.into(),
        offset: 1_048_576,
    };
    let binary_bytes = binary.encode_to_vec();
    let json_bytes = serde_json::to_vec(&json).unwrap();
    assert_eq!(Control::decode(binary_bytes.as_slice()).unwrap(), binary);
    assert_eq!(
        serde_json::from_slice::<JsonControl>(&json_bytes)
            .unwrap()
            .token,
        token
    );
    println!("codec,operation,p50_batch_mean_ns,p95_batch_mean_ns,body_bytes");
    let mut scratch = Vec::with_capacity(256);
    let (p50, p95) = sample(|| {
        scratch.clear();
        black_box(&binary).encode(black_box(&mut scratch)).unwrap();
        black_box(&scratch);
    });
    println!("bilrost,encode,{p50},{p95},{}", binary_bytes.len());
    let (p50, p95) = sample(|| {
        scratch.clear();
        serde_json::to_writer(black_box(&mut scratch), black_box(&json)).unwrap();
        black_box(&scratch);
    });
    println!("json-serde,encode,{p50},{p95},{}", json_bytes.len());
    let (p50, p95) = sample(|| {
        black_box(Control::decode(black_box(binary_bytes.as_slice())).unwrap());
    });
    println!("bilrost,decode,{p50},{p95},{}", binary_bytes.len());
    let (p50, p95) = sample(|| {
        black_box(serde_json::from_slice::<JsonControl>(black_box(&json_bytes)).unwrap());
    });
    println!("json-serde,decode,{p50},{p95},{}", json_bytes.len());
}
