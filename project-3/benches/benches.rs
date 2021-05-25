use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::{FromEntropy, Rng, SeedableRng, random};
use rand::rngs::SmallRng;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use criterion::BenchmarkId;
use tempfile::TempDir;

pub fn criterion_benchmark(c: &mut Criterion) {
    let seed: u64 = random();
    let kvs_dir = TempDir::new().expect("unable to create temporary working directory");
    let kvs_dir = kvs_dir.path();
    let sled_dir = TempDir::new().expect("unable to create temporary working directory");
    let sled_dir = sled_dir.path();
    
    c.bench_with_input(BenchmarkId::new("kvs_write", seed), &seed, 
    move |b, &seed| {
        let mut key_rng = SmallRng::seed_from_u64(seed);
        let mut value_rng = SmallRng::from_entropy();

        let mut store = KvStore::open(kvs_dir).unwrap();
        
	    b.iter(|| {
		    for _  in 0..100 {
                store.set(key_rng.gen_range(1, 100000).to_string(), 
                         value_rng.gen_range(1, 100000).to_string()).unwrap();
            }
		});
	});

    c.bench_with_input(BenchmarkId::new("kvs_read", seed), &seed, 
    move |b, &seed| {
        let mut key_rng = SmallRng::seed_from_u64(seed);

        let mut store = KvStore::open(kvs_dir).unwrap();
        
	    b.iter(|| {
		    for _  in 0..1000 {
                store.get(key_rng.gen_range(1, 100000).to_string()).unwrap();
            }
		});
	});

    c.bench_with_input(BenchmarkId::new("sled_write", seed), &seed, 
    move |b, &seed| {
        let mut key_rng = SmallRng::seed_from_u64(seed);
        let mut value_rng = SmallRng::from_entropy();

        let mut store = SledKvsEngine::open(sled_dir).unwrap();
        
	    b.iter(|| {
		    for _  in 0..100 {
                store.set(key_rng.gen_range(1, 100000).to_string(), 
                         value_rng.gen_range(1, 100000).to_string()).unwrap();
            }
		});
	});

    c.bench_with_input(BenchmarkId::new("sled_read", seed), &seed, 
    move |b, &seed| {
        let mut key_rng = SmallRng::seed_from_u64(seed);

        let mut store = SledKvsEngine::open(kvs_dir).unwrap();
        
	    b.iter(|| {
		    for _  in 0..1000 {
                store.get(key_rng.gen_range(1, 100000).to_string()).unwrap();
            }
		});
	});
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);