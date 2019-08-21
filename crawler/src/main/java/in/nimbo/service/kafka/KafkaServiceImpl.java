package in.nimbo.service.kafka;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.KafkaServiceException;
import in.nimbo.monitoring.ThreadsMonitor;
import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class KafkaServiceImpl implements KafkaService {
    private Logger logger = LoggerFactory.getLogger("crawler");
    private ScheduledExecutorService threadMonitorService;
    private KafkaConfig kafkaConfig;
    private CrawlerService crawlerService;
    private BlockingQueue<String> messageQueue;
    private CountDownLatch countDownLatch;

    private List<Thread> kafkaServices;
    private List<ProducerService> producerServices;
    private ConsumerService consumerService;

    public KafkaServiceImpl(CrawlerService crawlerService, KafkaConfig kafkaConfig) {
        this.crawlerService = crawlerService;
        this.kafkaConfig = kafkaConfig;
        kafkaServices = new ArrayList<>();
        messageQueue = new ArrayBlockingQueue<>(kafkaConfig.getLocalLinkQueueSize());
        countDownLatch = new CountDownLatch(kafkaConfig.getLinkProducerCount() + 1);
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        metricRegistry.register(MetricRegistry.name(KafkaServiceImpl.class, "localMessageQueueSize"),
                new CachedGauge<Integer>(1, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return messageQueue.size();
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
        ThreadGroup threadGroup = new ThreadGroup(kafkaConfig.getServiceName());
        startThreadsMonitoring(threadGroup);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getLinkConsumerProperties());
        kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getLinkTopic()));
        consumerService = new ConsumerServiceImpl(kafkaConfig, kafkaConsumer, messageQueue, countDownLatch);
        Thread consumerThread = new Thread(threadGroup, consumerService, kafkaConfig.getServiceName());
        kafkaServices.add(consumerThread);
        consumerThread.start();

        for (int i = 0; i < kafkaConfig.getLinkProducerCount(); i++) {
            KafkaProducer<String, String> shufflerProducer = new KafkaProducer<>(kafkaConfig.getShufflerProducerProperties());
            KafkaProducer<String, Page> pageProducer = new KafkaProducer<>(kafkaConfig.getPageProducerProperties());
            ProducerService pageProducerService = new ProducerServiceImpl(kafkaConfig, messageQueue,
                    pageProducer, shufflerProducer, crawlerService, countDownLatch);
            Thread pageProducerThread = new Thread(threadGroup, pageProducerService, kafkaConfig.getServiceName());
            kafkaServices.add(pageProducerThread);
            producerServices.add(pageProducerService);
            pageProducerThread.start();
        }
    }

    /**
     * stop services
     */
    @Override
    public void stopSchedule() {
        logger.info("Stop schedule service");
        consumerService.close();
        for (ProducerService producerService : producerServices) {
            producerService.close();
        }
        for (Thread service : kafkaServices) {
            service.interrupt();
        }
        try {
            countDownLatch.await();
            logger.info("All service stopped");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.getLinkProducerProperties())) {
                logger.info("Start sending {} messages from local message queue to kafka", messageQueue.size());
                for (String message : messageQueue) {
                    producer.send(new ProducerRecord<>(kafkaConfig.getLinkTopic(), message, message));
                }
                producer.flush();
            }
            logger.info("All messages sent to kafka");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        threadMonitorService.shutdown();
    }

    @Override
    public void sendMessage(String message) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.getLinkProducerProperties())) {
            producer.send(new ProducerRecord<>(kafkaConfig.getLinkTopic(), message, message));
            producer.flush();
        }
    }

    private void startThreadsMonitoring(ThreadGroup threadGroup) {
        ThreadsMonitor threadsMonitor = new ThreadsMonitor(threadGroup);
        threadMonitorService = Executors.newScheduledThreadPool(1);
        threadMonitorService.scheduleAtFixedRate(threadsMonitor, 0, 1, TimeUnit.SECONDS);
    }
}
