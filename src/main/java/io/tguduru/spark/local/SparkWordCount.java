package io.tguduru.spark.local;

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
 * A changed application to count the words which reads a file from a local file system and execute it.
 * @author Guduru, Thirupathi Reddy
 */
public class SparkWordCount {
    public static void main(String[] args) {
        List<String> stopWords = Arrays.asList("is", "the", "be", "It", "a", "in", "to", "will", "also");
        SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = javaSparkContext
                .textFile(SparkWordCount.class.getClassLoader().getResource("words.txt").getPath());
        JavaRDD<String> allWords = lines
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split("\\s+")).iterator()) // line split
                                                                                                           // into words
                .filter((Function<String, Boolean>) v1 -> v1 != null && v1.length() > 0) // filter blank/empty words
                .filter((Function<String, Boolean>) v1 -> !stopWords.contains(v1)); // filter stop words
        JavaPairRDD<String, Integer> words = allWords.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = words.reduceByKey((v1, v2) -> v1 + v2);
        counts.foreach(tuple -> System.out.println(tuple));
    }
}
