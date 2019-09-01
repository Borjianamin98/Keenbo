package in.nimbo.service;

import in.nimbo.config.ClassifierConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.util.Map;

public class ClassifierService {
    private ClassifierService() {
    }

    public static void classify(ClassifierConfig classifierConfig, SparkSession spark,
                                JavaPairRDD<String, Map<String, Object>> elasticSearchRDD) {

        NaiveBayesModel model = NaiveBayesModel.load(classifierConfig.getNaiveBayesModelSaveLocation());
        IDFModel idfModel = IDFModel.load(classifierConfig.getNaiveBayesIDFSaveLocation());

        JavaRDD<Row> dataRDD = elasticSearchRDD.map(tuple2 -> RowFactory.create(tuple2._1, tuple2._2.get("content")));
        StructType esStruct = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("content", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> dataset = spark.createDataFrame(dataRDD, esStruct);

        Tokenizer tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(dataset);

        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .setNumFeatures(classifierConfig.getHashingNumFeatures());
        Dataset<Row> featuredData = hashingTF.transform(wordsData);

        Dataset<Row> rescaledData = idfModel.transform(featuredData);
        Dataset<Row> features = rescaledData.select("id", "feature");

        JavaPairRDD<String, Double> predictionAndLabel =
                features.toJavaRDD().mapToPair((Row p) -> new Tuple2<>(p.getString(0), model.predict(p.getAs(1))));
        JavaRDD<Object> join = elasticSearchRDD.join(predictionAndLabel)
                .map(tuple2 -> {
                    Map<String, Object> map = tuple2._2._1;
                    map.put("id", tuple2._1);
                    map.put("label", tuple2._2._2);
                    return map;
                });

//        JavaEsSpark.saveToEs();

    }
}
