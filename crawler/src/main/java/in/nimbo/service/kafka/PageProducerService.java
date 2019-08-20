package in.nimbo.service.kafka;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.exception.ParseLinkException;
import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class PageProducerService implements ProducerService {
    private Logger logger = LoggerFactory.getLogger("crawler");
    private KafkaConfig config;
    private BlockingQueue<String> messageQueue;
    private BlockingQueue<String> shuffleQueue;
    private Producer<String, Page> pageProducer;
    private CrawlerService crawlerService;

    private CountDownLatch countDownLatch;
    private AtomicBoolean closed = new AtomicBoolean(false);

    private Counter allLinksCounter;

    public PageProducerService(KafkaConfig config,
                               BlockingQueue<String> messageQueue, BlockingQueue<String> shuffleQueue,
                               Producer<String, Page> pageProducer, CrawlerService crawlerService, CountDownLatch countDownLatch) {
        this.config = config;
        this.messageQueue = messageQueue;
        this.shuffleQueue = shuffleQueue;
        this.pageProducer = pageProducer;
        this.crawlerService = crawlerService;
        this.countDownLatch = countDownLatch;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        allLinksCounter = metricRegistry.counter(MetricRegistry.name(ProducerService.class, "allLinksCounter"));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                String newLink = messageQueue.take();
                handleLink(newLink);
                allLinksCounter.inc();
            }
        } catch (InterruptedException e) {
            // ignored
        } finally {
            if (pageProducer != null)
                pageProducer.close();
            logger.info("Page Producer service stopped successfully");
            countDownLatch.countDown();
        }
    }

    private void handleLink(String link) {
        try {
            Optional<Page> optionalPage = crawlerService.crawl(link);
            if (optionalPage.isPresent()) {
                Page page = optionalPage.get();
                for (Anchor anchor : page.getAnchors()) {
                    String anchorHref = anchor.getHref();
                    if (!anchorHref.contains("#")) {
                        shuffleQueue.put(anchorHref);
                    }
                }
                pageProducer.send(new ProducerRecord<>(config.getPageTopic(), page.getLink(), page));
            } else {
                shuffleQueue.put(link);
            }
        } catch (ParseLinkException | InvalidLinkException ignored) {
            // Ignore link
        } catch (Exception e) {
            logger.error("Uncached exception", e);
        }
    }
}
