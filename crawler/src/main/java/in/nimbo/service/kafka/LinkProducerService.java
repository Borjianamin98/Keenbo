package in.nimbo.service.kafka;

import in.nimbo.common.config.KafkaConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LinkProducerService implements ProducerService {
    private Logger logger = LoggerFactory.getLogger("crawler");
    private KafkaConfig config;
    private BlockingQueue<String> shuffleQueue;
    private Producer<String, String> linkProducer;
    private int maxShuffleQueueSize;

    private AtomicBoolean closed = new AtomicBoolean(false);
    private CountDownLatch countDownLatch;

    private ThreadLocalRandom random = ThreadLocalRandom.current();

    public LinkProducerService(KafkaConfig config, BlockingQueue<String> shuffleQueue, int maxShuffleQueueSize,
                               Producer<String, String> linkProducer, CountDownLatch countDownLatch) {
        this.config = config;
        this.shuffleQueue = shuffleQueue;
        this.maxShuffleQueueSize = maxShuffleQueueSize;
        this.linkProducer = linkProducer;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            int retry = 0;
            int lastQueueSize = -1;
            while (!closed.get()) {
                int size = shuffleQueue.size();
                if (size == maxShuffleQueueSize || retry >= 3) {
                    processQueue();
                    retry = 0;
                } else {
                    if (size == lastQueueSize) {
                        retry++;
                    } else {
                        retry = 0;
                    }
                }
                lastQueueSize = size;
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (linkProducer != null)
                linkProducer.close();

            logger.info("Link Producer stopped");
            countDownLatch.countDown();
        }
    }

    private String[] shuffle(BlockingQueue<String> shuffleQueue) {
        String[] arr = shuffleQueue.toArray(new String[0]);
        for (int i = shuffleQueue.size(); i > 1; i--) {
            swap(arr, i - 1, random.nextInt(i));
        }
        return arr;
    }

    private static void swap(String[] arr, int i, int j) {
        String tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private void processQueue() {
        String[] shuffledLinks = shuffle(shuffleQueue);
        for (String link : shuffledLinks) {
            linkProducer.send(new ProducerRecord<>(config.getLinkTopic(), link, link));
        }
        shuffleQueue.clear();
    }
}
