package org.tguduru.spark.rdd;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Demonstrates the operations of {@link org.apache.spark.api.java.JavaRDD}
 * @author Guduru, Thirupathi Reddy
 */
public class RDDOperationsDemo {
    public static void main(final String[] args) {
        final SparkConf sparkConf = new SparkConf();
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.parallelize(Lists.newArrayList(1,2,3,4,5,6,7,8));
    }
}
