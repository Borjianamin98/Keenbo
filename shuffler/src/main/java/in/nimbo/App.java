package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.redis.RedisDAOImpl;
import in.nimbo.service.kafka.KafkaService;
import in.nimbo.service.kafka.KafkaServiceImpl;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class App {
    private static final String INVALID_INPUT = "invalid input";
    private static Logger cliLogger = LoggerFactory.getLogger("cli");
    private static Logger appLogger = LoggerFactory.getLogger("shuffler");
    private KafkaService kafkaService;
    private RedissonClient redissonClient;


    public App(KafkaService kafkaService, RedissonClient redis) {
        this.kafkaService = kafkaService;
        this.redissonClient = redis;
    }

    public static void main(String[] args) {
        ProjectConfig projectConfig = ProjectConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();
        appLogger.info("Configuration loaded");

        initReporter(projectConfig);
        appLogger.info("Reporter started");

        Config config = new Config();
        config.useClusterServers()
                .addNodeAddress("redis://slave-1:6379")
                .addNodeAddress("redis://slave-2:6379")
                .addNodeAddress("redis://slave-3:6379");
        RedissonClient redis = Redisson.create(config);
        RedisDAOImpl redisDAO = new RedisDAOImpl(redis);

        KafkaService kafkaService = new KafkaServiceImpl(kafkaConfig, redisDAO);
        appLogger.info("Services started");

        appLogger.info("Application started");
        App app = new App(kafkaService, redis);
        Runtime.getRuntime().addShutdownHook(new Thread(app::stopApp));

        app.startApp();
    }

    private void startApp() {
        kafkaService.schedule();
        appLogger.info("Schedule service started");
        cliLogger.info("Welcome to Shuffler\n");
        cliLogger.info("shuffler> ");
        Scanner in = new Scanner(System.in);
        while (in.hasNext()) {
            String cmd = in.next();
            if (cmd.equals("exit")) {
                stopApp();
                break;
            } else {
                cliLogger.info(INVALID_INPUT + "\n");
            }
            cliLogger.info("shuffler> ");
        }
    }

    private void stopApp() {
        kafkaService.stopSchedule();
        redissonClient.shutdown();
        appLogger.info("Application stopped");
    }

    private static void initReporter(ProjectConfig projectConfig) {
        MetricRegistry metricRegistry = SharedMetricRegistries.setDefault(projectConfig.getReportName());
        JmxReporter reporter = JmxReporter.forRegistry(metricRegistry)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .convertRatesTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start();
    }
}
