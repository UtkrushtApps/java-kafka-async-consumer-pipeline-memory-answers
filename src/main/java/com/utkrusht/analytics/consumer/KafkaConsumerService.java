package com.utkrusht.analytics.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

@Service
public class KafkaConsumerService implements DisposableBean {
    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.consumer.topics}")
    private String topics;

    @Value("${kafka.consumer.max-poll-records:1000}")
    private int maxPollRecords;

    @Value("${kafka.consumer.fetch-min-bytes:1048576}")
    private int fetchMinBytes; // 1MB default

    @Value("${kafka.consumer.max-poll-interval-ms:300000}")
    private int maxPollIntervalMs;

    @Value("${kafka.consumer.threads:8}")
    private int consumerThreads;

    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;
    private ExecutorService processingPool;
    private List<Future<?>> processingFutures = new CopyOnWriteArrayList<>();
    
    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topics.split(",")));
        
        processingPool = new ThreadPoolExecutor(
                consumerThreads,
                consumerThreads*2,
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(maxPollRecords * 2),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        Thread consumerThread = new Thread(this::poll);
        consumerThread.setName("KafkaAsyncConsumerThread");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    private void poll() {
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                List<ConsumerRecord<String, String>> recordList = new ArrayList<>(records.count());
                for (ConsumerRecord<String, String> record : records) {
                    recordList.add(record);
                }
                // Dispatch batch processing asynchronously
                Future<?> fut = processingPool.submit(() -> {
                    processBatch(recordList);
                });
                processingFutures.add(fut);
                // Only commit offsets after all processing completed
                cleanAndCommit();
            }
        }
        // On shutdown, finish processing
        finishAndCommitPending();
    }

    private void processBatch(List<ConsumerRecord<String, String>> batch) {
        // Do your transformation logic here. Assume stateless for GC friendliness
        for (ConsumerRecord<String, String> record : batch) {
            try {
                // Example processing (simulate async transformation)
                AnalyticsTransformer.transform(record.value());
            } catch (Exception e) {
                // Log and handle errors at record-level
                // e.g., log.warn("Error processing record", e);
            }
        }
    }

    private void cleanAndCommit() {
        Iterator<Future<?>> it = processingFutures.iterator();
        while (it.hasNext()) {
            Future<?> fut = it.next();
            if (fut.isDone() || fut.isCancelled()) {
                try {
                    fut.get(90, TimeUnit.SECONDS); // Wait for processing (async)
                } catch (Exception e) {
                    // log.warn("Async processing failed", e);
                }
                it.remove();
                // After batch processes, commit offsets up-to-now
                try {
                    consumer.commitAsync();
                } catch (Exception e) {
                    // log.warn("CommitAsync failed", e);
                }
            }
        }
    }

    private void finishAndCommitPending() {
        for (Future<?> fut : processingFutures) {
            try { fut.get(); } catch (Exception ignored) {}
        }
        try { consumer.commitSync(); } catch (Exception ignored) {}
    }

    @Override
    public void destroy() {
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
        processingPool.shutdown();
        try {
            processingPool.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {}
        if (consumer != null) {
            try { consumer.close(); } catch (Exception ignored) {} 
        }
    }
}
