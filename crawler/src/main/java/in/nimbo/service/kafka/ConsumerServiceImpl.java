package in.nimbo.service.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerServiceImpl implements ConsumerService {
    private Logger logger = LoggerFactory.getLogger("crawler");
    private BlockingQueue<String> messageQueue;
    private Consumer<String, String> consumer;

    private CountDownLatch countDownLatch;
    private AtomicBoolean closed = new AtomicBoolean(false);

    public ConsumerServiceImpl(Consumer<String, String> consumer, BlockingQueue<String> messageQueue,
                               CountDownLatch countDownLatch) {
        this.consumer = consumer;
        this.messageQueue = messageQueue;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    messageQueue.put(record.value());
                }
            }
        } catch (InterruptedException e) {
            logger.info("Consumer service stopped successfully");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.info("Consumer service stopped with failures");
        } finally {
            if (consumer != null)
                consumer.close();
            countDownLatch.countDown();
        }
    }
}
