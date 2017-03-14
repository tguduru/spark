package io.tguduru.spark.cluster;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

/**
 * Executes a word count job on a spark cluster
 * @author Guduru, Thirupathi Reddy
 * @modified 3/13/17
 */
public class SparkWordCount {
    public static void main(final String[] args) {
        if (args.length != 1) {
            System.out.println("Command:");
            System.out.println(
                    "spark-submit --class io.tguduru.spark.local.SparkWordCount --master spark://M1600577:7077 target/spark-0.1-SNAPSHOT.jar src/main/resources/words.txt");
            System.exit(-1);
        }
        final List<String> stopWords = Arrays.asList("is", "the", "be", "It", "a", "in", "to", "will", "also");
        final SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("spark://M1600577:7077");
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        final JavaRDD<String> lines = javaSparkContext.textFile(args[0]);
        final JavaRDD<String> allWords = lines
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split("\\s+")).iterator()) // line split
                // into words
                .filter((Function<String, Boolean>) v1 -> v1 != null && v1.length() > 0) // filter blank/empty words
                .filter((Function<String, Boolean>) v1 -> !stopWords.contains(v1)); // filter stop words
        final JavaPairRDD<String, Integer> words = allWords.mapToPair(s -> new Tuple2<>(s, 1));
        final JavaPairRDD<String, Integer> counts = words.reduceByKey((v1, v2) -> v1 + v2);
        counts.foreach(tuple -> System.out.println(tuple));
    }
}
