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
                filter(s -> !s.getCancelled().equals("\"CANCELLED\""));

        System.out.println("--------ParsedData--------\n");
        parsedTotalData.take(5).forEach(s -> System.out.println(s.getOriginAirportID() + " " + s.getDestAirportID() + " " +
                s.getDelay() + " " + s.getCancelled()));
        System.out.println("------------------------\n");

        JavaPairRDD<Tuple2<String, String>, SingleStatistics> orderedTotalData = parsedTotalData.mapToPair(s ->
                new Tuple2<>(new Tuple2<>(s.getOriginAirportID(), s.getDestAirportID()),
                new SingleStatistics(s.getDelay(), s.getCancelled())));

        System.out.println("--------SingleStatistics--------\n");
        orderedTotalData.take(5).forEach(s -> System.out.println(s._2.getDelay() + " " + s._2.getCancelled()));
        System.out.println("------------------------\n");

        JavaPairRDD<Tuple2<String, String>, TotalStatistics> airportData = orderedTotalData.combineByKey(
                TotalStatistics::new,
                TotalStatistics::updateStatistics,
                TotalStatistics::update);

        System.out.println("--------TotalStatistics--------\n");
        airportData.take(5).forEach(s -> System.out.println(s._2.getMaxDelay() + " " + s._2.getTotalFlights() + " " +
                s._2.getTotalDelayedCancelledFlights() + " " + s._2.getPercentDelayedCancelledFlights()));
        System.out.println("------------------------\n");


        JavaRDD<ParsedNames> parsedAirportNames = airportNames.map(s -> new ParsedNames(s.split(",", 2))).
                filter(s -> !s.getAirportName().equals("Description"));
        Map<String, String> airportNamesMap = parsedAirportNames.mapToPair(s ->
                new Tuple2<>(s.getAirportID(), s.getAirportName())).collectAsMap();

        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airportNamesMap);
        JavaRDD<FinalAirportStatistics> airportStatistics = airportData.map(s -> new FinalAirportStatistics(s._1, s._2,
                airportsBroadcasted.value().get(s._1._1), airportsBroadcasted.value().get(s._1._2)));
        airportStatistics.saveAsTextFile("output");
    }
}
