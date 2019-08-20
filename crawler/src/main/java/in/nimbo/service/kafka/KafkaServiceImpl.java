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
    private BlockingQueue<String> shuffleQueue;
    private ConsumerService consumerService;
    private List<ProducerService> producerServices;
    private CountDownLatch countDownLatch;

    private List<Thread> kafkaServices;

    public KafkaServiceImpl(CrawlerService crawlerService, KafkaConfig kafkaConfig) {
        this.crawlerService = crawlerService;
        this.kafkaConfig = kafkaConfig;
        producerServices = new ArrayList<>();
        kafkaServices = new ArrayList<>();
        messageQueue = new ArrayBlockingQueue<>(kafkaConfig.getLocalLinkQueueSize());
        shuffleQueue = new ArrayBlockingQueue<>(1000000);
        countDownLatch = new CountDownLatch(kafkaConfig.getLinkProducerCount() + 2);
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        metricRegistry.register(MetricRegistry.name(KafkaServiceImpl.class, "localMessageQueueSize"),
                new CachedGauge<Integer>(3, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return messageQueue.size();
                    }
                });
        metricRegistry.register(MetricRegistry.name(KafkaServiceImpl.class, "localShuffleQueueSize"),
                new CachedGauge<Integer>(3, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return shuffleQueue.size();
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
        consumerService = new ConsumerServiceImpl(kafkaConsumer, messageQueue, countDownLatch);
        Thread consumerThread = new Thread(threadGroup, consumerService, kafkaConfig.getServiceName());
        kafkaServices.add(consumerThread);
        consumerThread.start();

        for (int i = 0; i < kafkaConfig.getLinkProducerCount(); i++) {
            KafkaProducer<String, Page> pageProducer = new KafkaProducer<>(kafkaConfig.getPageProducerProperties());
            ProducerService pageProducerService = new PageProducerService(kafkaConfig, messageQueue, shuffleQueue,
                    pageProducer, crawlerService, countDownLatch);
            producerServices.add(pageProducerService);
            Thread pageProducerThread = new Thread(threadGroup, pageProducerService, kafkaConfig.getServiceName());
            kafkaServices.add(pageProducerThread);
            pageProducerThread.start();
        }

        KafkaProducer<String, String> linkProducer = new KafkaProducer<>(kafkaConfig.getLinkProducerProperties());
        ProducerService linkProducerService = new LinkProducerService(kafkaConfig, shuffleQueue, linkProducer, countDownLatch);
        producerServices.add(linkProducerService);
        Thread linkProducerThread = new Thread(threadGroup, linkProducerService, kafkaConfig.getServiceName());
        kafkaServices.add(linkProducerThread);
        linkProducerThread.start();
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
        try {
            countDownLatch.await();
            logger.info("All service stopped");
            logger.info("Start sending {} messages to kafka", messageQueue.size());
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.getLinkProducerProperties())) {
                for (String message : messageQueue) {
                    producer.send(new ProducerRecord<>(kafkaConfig.getLinkTopic(), message, message));
                }
                for (String message : shuffleQueue) {
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
        threadMonitorService.scheduleAtFixedRate(threadsMonitor, 0, 10, TimeUnit.SECONDS);
    }
}
