package ru.bmstu.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class JoinJob {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Airport flight statistics");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> totalData = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportNames = sc.textFile("L_AIRPORT_ID.csv");

        JavaRDD<ParsedData> parsedTotalData = totalData.map(s -> new ParsedData(s.split(","))).filter(s -> s.getCancelled() != "CANCELLED");
        JavaPairRDD<Tuple2<String, String>, SingleStatistics> orderedTotalData = parsedTotalData.mapToPair(s ->
                new Tuple2<>(new Tuple2<>(s.getOriginAirportID(), s.getDestAirportID()),
                new SingleStatistics(s.getDelay(), s.getCancelled())));
        JavaPairRDD<Tuple2<String, String>, TotalStatistics> finalData =
                orderedTotalData.combineByKey(TotalStatistics::new,
                        (totalStatistics, s) -> TotalStatistics.updateStatistics(s),
                        TotalStatistics::update);
    }
}
