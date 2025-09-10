# Solution Steps

1. Define critical Kafka consumer configurations (max.poll.records, fetch.min.bytes, thread counts) in application.properties for better batch processing and throughput.

2. Create a service class KafkaConsumerService: in the init() method, create and configure a KafkaConsumer instance with all relevant tuning parameters, and subscribe to the analytics topics.

3. Set up a dedicated ExecutorService pool for concurrent batch processing, controlling concurrency for JVM stability.

4. In a background Thread, call poll() in a short interval loop, and for each set of polled records, offload batch processing to the ExecutorService so that consumer threads do not block on user logic.

5. Ensure each batch is processed separately and asynchronously, and commit Kafka offsets (using commitAsync) only after successful completion of each batch, avoiding over-commit and preventing unacknowledged message growth.

6. Clean up processing futures to prevent memory leaks via a CopyOnWriteArrayList and periodic removal of completed tasks.

7. Add a stateless transformation utility (AnalyticsTransformer) to handle record processing without growing heap usage or leaking references.

8. On shutdown, await completion of all pending batch-processing tasks and commit all processed offsets (commitSync).

9. Override destroy() to close all resources safely, halting polling, closing the consumer, and shutting down the ExecutorService.

