package org.tguduru.spark.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.Arrays;

/**
 * Demonstrates the execution of Spark Application by reading data from hadoop
 * @author Guduru, Thirupathi Reddy
 */
public class SparkHadoopWordCountApp {
    public static void main(final String[] args) {
        if (args.length < 2) {
            System.out
                    .println("Usage : spark-submit.sh --class org.tguduru.spark.hadoop.SparkHadoopWordCountApp --master <master-url> <uber-jar> <hdfs-input-path> <hdfs-output-path>");
            System.exit(1);
        }
        String hdfsRoot = "hdfs://localhost:8020";
        final String inputPath = args[0];
        final String outputPath = args[1];
        final SparkConf sparkConf = new SparkConf().setAppName("SparkHadoop").setMaster("spark://M1320945:7077");
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        // needs to append HDFS root path as user inputs only the file relative path.
        //reads the files from hadoop
        final JavaRDD<String> lines = javaSparkContext.textFile(hdfsRoot + inputPath);

        //transformations
        //produce a flatMap with words in a given line
        final JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(final String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
        //transformations
        System.out.println("***  Read Words ****");
        final JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(final String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        System.out.println("*** Pairs computed ****");
        //action
        final JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(final Integer integer1, final Integer integer2) throws Exception {
                return integer1 + integer2;
            }
        });
        System.out.println("*** Total Output Records - " + counts.count());
        //action
        counts.saveAsTextFile(hdfsRoot + outputPath);
    }
}
