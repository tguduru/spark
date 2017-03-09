package org.tguduru.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * Demonstrates the operations of {@link org.apache.spark.api.java.JavaRDD}
 * @author Guduru, Thirupathi Reddy
 */
public class RDDOperationsDemo {
    public static void main(final String[] args) {
        final SparkConf sparkConf = new SparkConf().setAppName("SparkHadoop").setMaster("spark://M1600577:7077");
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        LongAccumulator longAccumulator = javaSparkContext.sc().longAccumulator();
        List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        javaSparkContext.parallelize(integers).distinct().collect().forEach(longAccumulator::add);
        longAccumulator.value();
    }
}
