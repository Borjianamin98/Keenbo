package in.nimbo.service;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.common.config.KafkaConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShufflerService implements Runnable, Closeable {
    private Logger logger = LoggerFactory.getLogger("shuffler");
    private List<String> shuffleList;
    private KafkaConfig kafkaConfig;
    private Consumer<String, String> shufflerConsumer;
    private Producer<String, String> linkProducer;
    private int maxShuffleQueueSize;

    private AtomicBoolean closed = new AtomicBoolean(false);
    private CountDownLatch countDownLatch;
    private ThreadLocalRandom random = ThreadLocalRandom.current();

    private Timer shuffleLinksTimer;

    public ShufflerService(KafkaConfig kafkaConfig, int maxShuffleQueueSize,
                           Consumer<String, String> shufflerConsumer, Producer<String, String> linkProducer,
                           List<String> shuffleList, CountDownLatch countDownLatch) {
        this.kafkaConfig = kafkaConfig;
        this.maxShuffleQueueSize = maxShuffleQueueSize;
        this.shufflerConsumer = shufflerConsumer;
        this.linkProducer = linkProducer;
        this.shuffleList = shuffleList;
        this.countDownLatch = countDownLatch;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        shuffleLinksTimer = metricRegistry.timer(MetricRegistry.name(ShufflerService.class, "shuffleLinksTimer"));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            int retry = 0;
            int lastSize = -1;
            while (!closed.get()) {
                ConsumerRecords<String, String> records = shufflerConsumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    shuffleList.add(record.value());
                }
                int size = shuffleList.size();
                if (size == maxShuffleQueueSize || retry >= 100) {
                    processList();
                    retry = 0;
                } else {
                    if (size == lastSize) {
                        retry++;
                    } else {
                        retry = 0;
                    }
                }
                lastSize = size;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (shufflerConsumer != null)
                shufflerConsumer.close();
            if (linkProducer != null)
                linkProducer.close();

            logger.info("Shuffler service stopped");
            countDownLatch.countDown();
        }
    }

    private String[] shuffle(List<String> shuffleList) {
        String[] arr = shuffleList.toArray(new String[0]);
        for (int i = shuffleList.size(); i > 1; i--) {
            swap(arr, i - 1, random.nextInt(i));
        }
        return arr;
    }

    private static void swap(String[] arr, int i, int j) {
        String tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private void processList() {
        Timer.Context shuffleLinksTimerContext = shuffleLinksTimer.time();
        String[] shuffledLinks = shuffle(shuffleList);
        shuffleLinksTimerContext.stop();
        for (String link : shuffledLinks) {
            linkProducer.send(new ProducerRecord<>(kafkaConfig.getLinkTopic(), link, link));
        }
        shuffleList.clear();
    }
}
