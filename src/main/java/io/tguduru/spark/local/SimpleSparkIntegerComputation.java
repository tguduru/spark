package io.tguduru.spark.local;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * Simple standalone Spark application.
 *
 * @author Guduru, Thirupathi Reddy.
 */
public class SimpleSparkIntegerComputation {
    public static void main(final String[] args) {
        // when setting master as "local" it won't submit the job and executes as a standalone mode.
        final SparkConf sparkConf = new SparkConf().setAppName("SparkApp").setMaster("local");
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        final List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        final JavaRDD<Integer> integerJavaRDD = javaSparkContext.parallelize(integers);
        final JavaRDD<Integer> even = integerJavaRDD.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });

        System.out.println(even.collect());
    }
}
