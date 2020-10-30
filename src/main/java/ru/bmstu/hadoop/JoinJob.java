package ru.bmstu.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JoinJob {
    SparkConf conf = new SparkConf().setAppName("Airport flight statistics");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> TotalInformationFile = sc.textFile("664600583_T_ONTIME_sample.csv");
    
}
