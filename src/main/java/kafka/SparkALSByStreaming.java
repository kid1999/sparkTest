package kafka;

import java.util.*;

import model.RatingEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import org.apache.spark.streaming.kafka010.LocationStrategies;

/**
 * @category 基于Spark-streaming、kafka的实时推荐模板DEMO 原系统中包含商城项目、logback、flume、hadoop
 * The real time recommendation template DEMO based on Spark-streaming and Kafka contains the mall project, logback, flume and Hadoop in the original system
 * @author huangyueran
 *
 */
public final class SparkALSByStreaming {

    //	基于Hadoop、Flume、Kafka、spark-streaming、logback、商城系统的实时推荐系统DEMO
    //	Real time recommendation system DEMO based on Spark, Kafka, spark-streaming, logback and mall system
    //	商城系统采集的数据集格式 Data Format:
    //	用户ID，商品ID，用户行为评分(0-5)
    //	UserID,ItemId,Rating


    public static void main(String[] args) throws InterruptedException {
        String brokers = "159.75.6.26:9092";
        String topic = "Test";


        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");


        // TODO 原始数据
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        final JavaRDD<Rating> originalData = jsc.textFile("src/main/resources/data/data.txt")
                .map(new Function<String, Rating>() {
                         @Override
                         public Rating call(String str) {
                             return RatingEntry.parseRating(str);
                         }
                     });


         // TODO 每隔5秒钟，咱们的spark streaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(
                jsc, Durations.seconds(5));


        // TODO 基于kafka direct api模式，构建Kafka参数
        // 两个值，key，value；key没有什么特殊的意义；value中包含了kafka topic中的一条一条的实时日志数据
        HashMap<String, Object> kafkaParams = new HashMap<>();
        //Kafka服务监听端口
        kafkaParams.put("bootstrap.servers", brokers);
        //指定kafka输出key的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        //指定kafka输出value的数据类型及编码格式（默认为字符串类型编码格式为uft-8）
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        //消费者ID，随意指定
        kafkaParams.put("group.id", "jis");
        //指定从latest(最新,其他版本的是largest这里不行)还是smallest(最早)处开始读取数据
        kafkaParams.put("auto.offset.reset", "latest");
        //如果true,consumer定期地往zookeeper写入每个分区的offset
        kafkaParams.put("enable.auto.commit", false);

        // TODO 设置参数和topic，读取kafka消息流
        Collection<String> topics = Arrays.asList(topic);
        final JavaInputDStream<ConsumerRecord<Integer, Integer>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics,kafkaParams)
                );

        // TODO 获取 kafka流数据
        JavaDStream<Rating> dStream = stream.map(new Function<ConsumerRecord<Integer, Integer>, Rating>() {
            @Override
            public Rating call(ConsumerRecord<Integer, Integer> record) throws Exception {
                return new Rating(record.key(),record.value(),4.0f);
            }
        });

        // TODO ==================================  开始计算 =================================================
        dStream.foreachRDD(rdd ->{
            // TODO 将原始数据集和新的用户行为数据进行合并
            JavaRDD<Rating> calculations = originalData.union(rdd);
            System.out.println("originalTextFile count:" + calculations.count());
            // TODO 设置ALS : 数据,维度,迭代次数,正则化参数
            MatrixFactorizationModel model = ALS.train(calculations.rdd(), 10, 6, 0.01);
            double predict = model.predict(1, 4);
            System.out.println("predict :" + predict);
            Rating[] ratings = model.recommendProducts(4, 3);
            for(Rating rating:ratings){
                System.out.println(rating.toString());
            }
        });

        // TODO ====================================== 结束计算 =============================================
        jssc.start();
        jssc.awaitTermination();
    }
}