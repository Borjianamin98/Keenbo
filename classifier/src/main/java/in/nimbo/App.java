package in.nimbo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.config.ProjectConfig;
import in.nimbo.common.dao.elastic.ElasticDAO;
import in.nimbo.common.dao.elastic.ElasticDAOImpl;
import in.nimbo.common.entity.Link;
import in.nimbo.common.service.ParserService;
import in.nimbo.common.utility.LanguageDetectorUtility;
import in.nimbo.common.utility.SparkUtility;
import in.nimbo.config.ClassifierConfig;
import in.nimbo.entity.Category;
import in.nimbo.service.ClassifierService;
import in.nimbo.service.TrainService;
import in.nimbo.service.CrawlerService;
import in.nimbo.service.kafka.KafkaServiceImpl;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class App {
    private static Logger logger = LoggerFactory.getLogger("classifier");
    private static ClassifierConfig classifierConfig;

    public static void main(String[] args) throws IOException {
        ProjectConfig projectConfig = ProjectConfig.load();
        classifierConfig = ClassifierConfig.load();
        if (classifierConfig.getAppMode() == ClassifierConfig.MODE.CRAWL) {
            runCrawler(projectConfig);
        } else if (classifierConfig.getAppMode() == ClassifierConfig.MODE.TRAIN) {
            runTrainService();
        } else if (classifierConfig.getAppMode() == ClassifierConfig.MODE.CLASSIFY) {
            runClassifier();
        }
    }

    private static void runCrawler(ProjectConfig projectConfig) throws IOException {
        LanguageDetectorUtility.loadLanguageDetector(logger);

        Cache<String, LocalDateTime> politenessCache = Caffeine.newBuilder().maximumSize(projectConfig.getCaffeineMaxSize())
                .expireAfterWrite(projectConfig.getCaffeineExpireTime(), TimeUnit.SECONDS).build();
        Cache<String, LocalDateTime> crawlerCache = Caffeine.newBuilder().build();

        ElasticConfig elasticConfig = ElasticConfig.load();
        KafkaConfig kafkaConfig = KafkaConfig.load();

        ElasticDAO elasticDAO = ElasticDAOImpl.createElasticDAO(elasticConfig, new CopyOnWriteArrayList<>());

        ObjectMapper mapper = new ObjectMapper();
        List<Category> categories = CrawlerService.loadFeed(mapper);
        Map<String, Double> labelMap = CrawlerService.loadLabels(categories);
        List<String> domains = CrawlerService.loadDomains(categories);
        BlockingQueue<Link> queue = new ArrayBlockingQueue<>(classifierConfig.getCrawlerQueueSize());
        CrawlerService.fillInitialCrawlQueue(queue, categories);

        ParserService parserService = new ParserService(projectConfig);
        CrawlerService crawlerService = new CrawlerService(politenessCache, crawlerCache, parserService, elasticDAO, labelMap);
        KafkaServiceImpl kafkaService = new KafkaServiceImpl(kafkaConfig, classifierConfig, crawlerService);
        kafkaService.schedule();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaService::stopSchedule));
    }

    private static void runTrainService() {
        SparkSession spark = setSparkEsConfigs();
        JavaPairRDD<String, Map<String, Object>> elasticSearchRDD =
                SparkUtility.getElasticSearchRDD(spark, classifierConfig.getEsIndex(), classifierConfig.getEsType());
        TrainService.extractModel(classifierConfig, spark, elasticSearchRDD);
        spark.stop();
    }

    private static void runClassifier() {
        SparkSession spark = setSparkEsConfigs();
        JavaPairRDD<String, Map<String, Object>> elasticSearchRDD =
                SparkUtility.getElasticSearchRDD(spark, classifierConfig.getEsIndex(), classifierConfig.getEsType());
        System.out.println(elasticSearchRDD.collectAsMap().toString());
//        ClassifierService.classify(classifierConfig, spark, elasticSearchRDD);
        spark.stop();
    }

    private static SparkSession setSparkEsConfigs() {
        SparkSession spark = SparkUtility.getSpark(classifierConfig.getAppName() + "-"
                + classifierConfig.getAppMode(), true);
        spark.sparkContext().conf().set("es.nodes", classifierConfig.getEsNodes());
        spark.sparkContext().conf().set("es.write.operation", classifierConfig.getEsWriteOperation());
        spark.sparkContext().conf().set("es.mapping.id", "id");
        spark.sparkContext().conf().set("es.index.auto.create", classifierConfig.getEsIndexAutoCreate());
        return spark;
    }
}
