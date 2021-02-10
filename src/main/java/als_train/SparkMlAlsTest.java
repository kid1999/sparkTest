package als_train;

import org.apache.spark.api.java.*;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;

/**
 * @author kid1999
 * @create 2021-02-09 9:36
 * @description TODO
 **/
public class SparkMlAlsTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().
                setAppName("Java Collaborative Filtering Example")
                .setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Load and parse the data
        String path = "src/main/resources/data.txt";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Rating> ratings = data.map(s -> {
            String[] sarray = s.split(",");
            return new Rating(Integer.parseInt(sarray[0]),
                    Integer.parseInt(sarray[1]),
                    Double.parseDouble(sarray[2]));
        });

        // Build the recommendation model using ALS
        int rank = 10;
        int numIterations = 10;
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

        double predict = model.predict(1, 2);
        System.out.println(predict);

    }

}