package ru.bmstu.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class JoinJob implements Serializable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Airport flight statistics");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> totalInformationFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> totalInformationFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String, String> airports =
    }
}
