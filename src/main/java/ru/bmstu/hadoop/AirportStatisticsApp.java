package ru.bmstu.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class JoinJob {
    private static final String DELIMITER = ",", FIRST_ROW_TOTAL_DATA = "\"CANCELLED\"",
            FIRST_ROW_AIRPORT_NAMES = "Description";
    private static final int NUMBER_OF_ARRAYS = 2;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Airport flight statistics");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> totalData = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportNames = sc.textFile("L_AIRPORT_ID.csv");

        JavaRDD<ParsedData> parsedTotalData = totalData.map(s -> new ParsedData(s.split(DELIMITER))).
                filter(s -> !s.getCancelled().equals(FIRST_ROW_TOTAL_DATA));
        JavaPairRDD<Tuple2<String, String>, SingleStatistics> orderedTotalData = parsedTotalData.mapToPair(s ->
                new Tuple2<>(new Tuple2<>(s.getOriginAirportID(), s.getDestAirportID()),
                new SingleStatistics(s.getDelay(), s.getCancelled())));
        JavaPairRDD<Tuple2<String, String>, TotalStatistics> airportData = orderedTotalData.combineByKey(
                TotalStatistics::new,
                TotalStatistics::updateStatistics,
                TotalStatistics::update);

        JavaRDD<ParsedNames> parsedAirportNames = airportNames.map(s -> new ParsedNames(s.split(DELIMITER, NUMBER_OF_ARRAYS))).
                filter(s -> !s.getAirportName().equals(FIRST_ROW_AIRPORT_NAMES));
        Map<String, String> airportNamesMap = parsedAirportNames.mapToPair(s ->
                new Tuple2<>(s.getAirportID(), s.getAirportName())).collectAsMap();

        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airportNamesMap);
        JavaRDD<FinalAirportStatistics> airportStatistics = airportData.map(s -> new FinalAirportStatistics(s._1, s._2,
                airportsBroadcasted.value().get(s._1._1), airportsBroadcasted.value().get(s._1._2)));
        airportStatistics.saveAsTextFile("output");
    }
}
