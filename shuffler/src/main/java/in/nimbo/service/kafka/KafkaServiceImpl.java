package in.nimbo.service.kafka;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.exception.KafkaServiceException;
import in.nimbo.service.ShufflerService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaServiceImpl implements KafkaService {
    private Logger logger = LoggerFactory.getLogger("collector");
    private KafkaConfig config;
    private ShufflerService shufflerService;
    private List<String> shuffleList;

    private CountDownLatch countDownLatch;

    public KafkaServiceImpl(KafkaConfig kafkaConfig) {
        this.config = kafkaConfig;
        countDownLatch = new CountDownLatch(1);
        shuffleList = new ArrayList<>();
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        metricRegistry.register(MetricRegistry.name(KafkaServiceImpl.class, "localShuffleQueueSize"),
                new CachedGauge<Integer>(1, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return shuffleList.size();
                    }
                });
    }

    /**
     * prepare kafka producer and consumer services and start threads to send/receive messages
     *
     * @throws KafkaServiceException if unable to prepare services
     */
    @Override
    public void schedule() {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(config.getShufflerConsumerProperties());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(config.getLinkProducerProperties());
        kafkaConsumer.subscribe(Collections.singletonList(config.getShufflerTopic()));
        shufflerService = new ShufflerService(config, config.getLocalShuffleQueueSize(),
                kafkaConsumer, kafkaProducer, shuffleList, countDownLatch);
        Thread shufflerServiceThread = new Thread(shufflerService);
        shufflerServiceThread.start();
    }

    /**
     * stop services
     */
    @Override
    public void stopSchedule() {
        logger.info("Stop schedule service");
        shufflerService.close();
        try {
            countDownLatch.await();
            logger.info("All service stopped");
            logger.info("Start sending {} messages to kafka", shuffleList.size());
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(config.getShufflerProducerProperties())) {
                for (String link : shuffleList) {
                    producer.send(new ProducerRecord<>(config.getShufflerTopic(), link, link));
                }
                producer.flush();
            }
            logger.info("All messages sent to kafka");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
