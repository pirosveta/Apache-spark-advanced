package ru.bmstu.hadoop;

import scala.Tuple2;

public class FinalAirportStatistics {
    private String originAirportID, originAirportName, destAirportID, destAirportName;
    private int maxDelay;
    private double percentDelayedAndCancelledFlights;

    public FinalAirportStatistics(Tuple2<String, String> airportID, TotalStatistics statistics,
                                  String originAirportName, String destAirportName) {
        originAirportID = airportID._1;
        destAirportID = airportID._2;
        this.originAirportName = originAirportName;
        this.destAirportName = destAirportName;
        maxDelay = statistics.getMaxDelay();
        percentDelayedAndCancelledFlights = statistics.getPercentDelayedCancelledFlights();
    }
}
