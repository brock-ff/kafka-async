use futures::stream::FuturesUnordered;
use futures::StreamExt;
use rdkafka::client::ClientContext;
use rdkafka::error::KafkaResult;
use rdkafka::message::Headers;
use rdkafka::topic_partition_list::TopicPartitionList;

use rdkafka::{
    config::ClientConfig,
    consumer::stream_consumer::StreamConsumer,
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance},
    Message,
};

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume(broker: &str, group_id: &str, topic: &str, process_id: usize) {
    println!("[process {}] spawning consumer", process_id);
    let context = CustomContext;
    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", broker)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create_with_context(context)
        .expect("Consumer creation failed");
    consumer.subscribe(&[topic]).expect("subscribe failed");

    let mut stream_processor = consumer.start();
    while let Some(message) = stream_processor.next().await {
        match message {
            Err(e) => println!("kafka error {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        println!("error deserializing message payload {:?}", e);
                        ""
                    }
                };
                // process message here
                println!("[process {}] key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      process_id, m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if let Some(headers) = m.headers() {
                    for i in 0..headers.count() {
                        let header = headers.get(i).unwrap();
                        println!("header {:#?}: {:?}", header.0, header.1);
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

#[tokio::main]
async fn main() {
    let num_workers: usize = 8;
    let broker = "localhost:9092";
    let group_id = "group";
    let topic = "app.async";
    let mut i: usize = 0;

    (0..num_workers)
        .map(|_| {
            tokio::spawn({
                i += 1;
                consume(broker, group_id, topic, i)
            })
        })
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}
