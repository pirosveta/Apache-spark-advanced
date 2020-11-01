package ru.bmstu.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class JoinJob {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Airport flight statistics");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> totalData = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportNames = sc.textFile("L_AIRPORT_ID.csv");

        JavaRDD<ParsedData> parsedTotalData = totalData.map(s -> new ParsedData(s.split(","))).
                filter(s -> s.getCancelled() != "CANCELLED");
        JavaPairRDD<Tuple2<String, String>, SingleStatistics> orderedTotalData = parsedTotalData.mapToPair(s ->
                new Tuple2<>(new Tuple2<>(s.getOriginAirportID(), s.getDestAirportID()),
                new SingleStatistics(s.getDelay(), s.getCancelled())));
        JavaPairRDD<Tuple2<String, String>, TotalStatistics> airportData = orderedTotalData.combineByKey(
                TotalStatistics::new,
                TotalStatistics::updateStatistics,
                TotalStatistics::update);

        JavaRDD<ParsedNames> parsedAirportNames = airportNames.map(s -> new ParsedNames(s.split(",", 2))).
                filter(s -> s.getAirportName() != "Description");
        Map<String, String> airportNamesMap = parsedAirportNames.mapToPair(s ->
                new Tuple2<>(s.getAirportID(), s.getAirportName())).collectAsMap();
        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airportNamesMap);
        JavaPairRDD<FinalAirportStatistics>
    }
}
