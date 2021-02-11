package es;

import avro.shaded.com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.spark_project.guava.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kid1999
 * @create 2021-02-11 9:18
 * @description TODO
 **/
public class sparkWithES {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("writeES")
                .setMaster("local[2]")
                .set("es.index.auto.create", "true")
                .set("es.nodes", "159.75.6.26")
                .set("es.port", "9200")
                .set("es.nodes.wan.only", "true");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        String target = "spark-test/_doc";
        List<Integer> nums = new ArrayList<>();
        for (int i = 0; i <10 ; i++)  nums.add(i);
        Map<String, ?> doc2 = ImmutableMap.of("one", 1,"goodsId",nums);
        JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(doc2));
        JavaEsSpark.saveToEs(javaRDD, target);
    }
}