Storing Parquet data directly in Kafka messages is an interesting approach. Here are the most performant strategies:

## 1. Direct Parquet Bytes in Kafka (Most Efficient)

```rust
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::consumer::{Consumer, StreamConsumer};
use polars::prelude::*;
use std::io::Cursor;

// Producer: Write DataFrame to Parquet bytes
async fn send_parquet_to_kafka(df: DataFrame, topic: &str, producer: &FutureProducer) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);

    // Write DataFrame directly to bytes
    ParquetWriter::new(cursor)
        .finish(&mut df.clone())?;

    let record = FutureRecord::to(topic)
        .payload(&buffer)
        .key("parquet_data");

    producer.send(record, None).await?;
    Ok(())
}

// Consumer: Read Parquet bytes directly to DataFrame
async fn consume_parquet_from_kafka(consumer: &StreamConsumer) -> PolarsResult<DataFrame> {
    match consumer.recv().await {
        Ok(message) => {
            if let Some(payload) = message.payload() {
                let cursor = Cursor::new(payload);
                let df = LazyFrame::scan_parquet(cursor, ScanArgsParquet::default())?
                    .collect()?;
                Ok(df)
            } else {
                Err(PolarsError::NoData("Empty message".into()))
            }
        }
        Err(e) => Err(PolarsError::Io(Arc::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Kafka error: {}", e)
        ))))
    }
}
```

## 2. Optimized Batch Consumer

```rust
use tokio::time::{timeout, Duration};

async fn consume_parquet_batch(
    consumer: &StreamConsumer,
    batch_size: usize,
    timeout_ms: u64
) -> PolarsResult<Vec<DataFrame>> {
    let mut dataframes = Vec::with_capacity(batch_size);
    let timeout_duration = Duration::from_millis(timeout_ms);

    for _ in 0..batch_size {
        match timeout(timeout_duration, consumer.recv()).await {
            Ok(Ok(message)) => {
                if let Some(payload) = message.payload() {
                    let cursor = Cursor::new(payload);
                    let df = LazyFrame::scan_parquet(cursor, ScanArgsParquet::default())?
                        .collect()?;
                    dataframes.push(df);
                }
            }
            _ => break, // Timeout or error, return what we have
        }
    }

    Ok(dataframes)
}

// Combine all DataFrames efficiently
async fn process_parquet_batch(consumer: &StreamConsumer) -> PolarsResult<DataFrame> {
    let batch = consume_parquet_batch(consumer, 100, 1000).await?;

    if batch.is_empty() {
        return Err(PolarsError::NoData("No data received".into()));
    }

    // Use concat for efficient combining
    let lazy_frames: Vec<LazyFrame> = batch
        .into_iter()
        .map(|df| df.lazy())
        .collect();

    polars::functions::concat(lazy_frames, UnionArgs::default())?
        .collect()
}
```

## 3. Streaming with Memory Management

```rust
use std::sync::Arc;
use tokio::sync::mpsc;

struct ParquetStreamProcessor {
    consumer: StreamConsumer,
    buffer_size: usize,
    max_memory_mb: usize,
}

impl ParquetStreamProcessor {
    async fn process_stream<F>(&self, mut processor: F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(DataFrame) -> PolarsResult<()>,
    {
        let (tx, mut rx) = mpsc::channel(self.buffer_size);

        // Spawn consumer task
        let consumer = Arc::new(self.consumer.clone());
        tokio::spawn(async move {
            loop {
                match consumer.recv().await {
                    Ok(message) => {
                        if let Some(payload) = message.payload() {
                            if tx.send(payload.to_vec()).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        // Process messages
        while let Some(parquet_bytes) = rx.recv().await {
            let cursor = Cursor::new(&parquet_bytes);
            let df = LazyFrame::scan_parquet(cursor, ScanArgsParquet::default())?
                .collect()?;

            processor(df)?;
        }

        Ok(())
    }
}
```

## 4. Compressed Parquet for Kafka

```rust
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use flate2::Compression;
use std::io::Write;

// Producer with compression
async fn send_compressed_parquet(df: DataFrame, topic: &str, producer: &FutureProducer) -> Result<(), Box<dyn std::error::Error>> {
    // First write to parquet bytes
    let mut parquet_buffer = Vec::new();
    let cursor = Cursor::new(&mut parquet_buffer);
    ParquetWriter::new(cursor).finish(&mut df.clone())?;

    // Then compress
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&parquet_buffer)?;
    let compressed = encoder.finish()?;

    let record = FutureRecord::to(topic)
        .payload(&compressed)
        .key("compressed_parquet");

    producer.send(record, None).await?;
    Ok(())
}

// Consumer with decompression
async fn consume_compressed_parquet(consumer: &StreamConsumer) -> PolarsResult<DataFrame> {
    match consumer.recv().await {
        Ok(message) => {
            if let Some(payload) = message.payload() {
                // Decompress
                let mut decoder = GzDecoder::new(payload);
                let mut decompressed = Vec::new();
                std::io::Read::read_to_end(&mut decoder, &mut decompressed)
                    .map_err(|e| PolarsError::Io(Arc::new(e)))?;

                // Read parquet
                let cursor = Cursor::new(decompressed);
                LazyFrame::scan_parquet(cursor, ScanArgsParquet::default())?
                    .collect()
            } else {
                Err(PolarsError::NoData("Empty message".into()))
            }
        }
        Err(e) => Err(PolarsError::Io(Arc::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Kafka error: {}", e)
        ))))
    }
}
```

## 5. Kafka Configuration for Parquet

```rust
use rdkafka::config::ClientConfig;

fn create_optimized_producer() -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.max.bytes", "10485760") // 10MB for larger parquet files
        .set("batch.size", "1048576") // 1MB batches
        .set("linger.ms", "100") // Small delay for batching
        .set("compression.type", "lz4") // Fast compression
        .set("acks", "1") // Balance between performance and durability
        .create()
        .expect("Producer creation error")
}

fn create_optimized_consumer() -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "parquet-consumer")
        .set("fetch.min.bytes", "1048576") // 1MB minimum fetch
        .set("fetch.max.wait.ms", "100")
        .set("max.partition.fetch.bytes", "10485760") // 10MB max
        .set("enable.auto.commit", "false") // Manual commit for better control
        .create()
        .expect("Consumer creation error")
}
```

## Performance Tips:

1. **Message Size**: Keep parquet chunks reasonably sized (1-10MB) to avoid Kafka limits
2. **Compression**: Use application-level compression (gzip/lz4) on top of Parquet's built-in compression
3. **Batching**: Process multiple messages together using `concat()` for better performance
4. **Memory Management**: Monitor memory usage when processing large parquet files
5. **Kafka Settings**: Tune `fetch.min.bytes`, `batch.size`, and compression settings
6. **Schema Consistency**: Ensure all parquet files have compatible schemas for efficient concatenation

This approach gives you the benefits of Polars' columnar processing with Kafka's streaming capabilities while maintaining high performance through direct binary transfer of parquet data.
