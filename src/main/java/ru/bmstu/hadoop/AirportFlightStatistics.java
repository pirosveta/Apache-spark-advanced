package ru.bmstu.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class AirportFlightStatistics {
    private static final String OUTPUT_FILE = "output", TOTAL_DATA_FILE = "664600583_T_ONTIME_sample.csv",
            AIRPORT_NAMES_FILE = "L_AIRPORT_ID.csv", DELIMITER = ",", ZERO_ROW_TOTAL_DATA = "\"CANCELLED\"",
            ZERO_ROW_AIRPORT_NAMES = "Description";
    private static final int NUMBER_OF_ARRAYS = 2;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Airport flight statistics");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> flightData = sc.textFile(TOTAL_DATA_FILE);
        JavaRDD<String> airportNames = sc.textFile(AIRPORT_NAMES_FILE);

        JavaRDD<ParsedData> parsedFlightData = flightData.map(s -> new ParsedData(s.split(DELIMITER))).
                filter(s -> !s.getCancelled().equals(ZERO_ROW_TOTAL_DATA));
        JavaPairRDD<Tuple2<String, String>, Statistics> orderedFlightData = parsedFlightData.mapToPair(s ->
                new Tuple2<>(new Tuple2<>(s.getOriginAirportID(), s.getDestAirportID()),
                new Statistics(s.getDelay(), s.getCancelled())));
        JavaPairRDD<Tuple2<String, String>, FilteredStatistics> filteredFlightData = orderedFlightData.combineByKey(
                FilteredStatistics::new,
                FilteredStatistics::updateStatistics,
                FilteredStatistics::update);

        JavaRDD<ParsedNames> parsedAirportNames = airportNames.map(s -> new ParsedNames(s.split(DELIMITER, NUMBER_OF_ARRAYS))).
                filter(s -> !s.getAirportName().equals(ZERO_ROW_AIRPORT_NAMES));
        Map<String, String> parsedAirportNamesMap = parsedAirportNames.mapToPair(s ->
                new Tuple2<>(s.getAirportID(), s.getAirportName())).collectAsMap();

        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(parsedAirportNamesMap);
        JavaRDD<TotalStatistics> airportStatistics = filteredFlightData.map(s -> new TotalStatistics(s._1, s._2,
                airportsBroadcasted.value().get(s._1._1), airportsBroadcasted.value().get(s._1._2)));
        airportStatistics.saveAsTextFile(OUTPUT_FILE);
    }
}
