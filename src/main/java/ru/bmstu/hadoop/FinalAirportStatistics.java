package ru.bmstu.hadoop;

import scala.Serializable;
import scala.Tuple2;

public class FinalAirportStatistics implements Serializable {
    private String originAirportID, originAirportName, destAirportID, destAirportName;
    private double maxDelay, percentDelayedAndCancelledFlights;

    public FinalAirportStatistics(Tuple2<String, String> airportID, TotalStatistics statistics,
                                  String originAirportName, String destAirportName) {
        originAirportID = airportID._1;
        destAirportID = airportID._2;
        this.originAirportName = originAirportName;
        this.destAirportName = destAirportName;
        maxDelay = statistics.getMaxDelay();
        percentDelayedAndCancelledFlights = statistics.getPercentDelayedCancelledFlights();
    }

    @Override
    public String toString() {
        return originAirportID + ' ' +
                originAirportName + ' ' +
                "- " + destAirportID + ' ' +
                destAirportName + ' ' +
                ": maxDelay = " + maxDelay +
                "; percentDelayedAndCancelledFlights = " + percentDelayedAndCancelledFlights +
                ';';
    }
}
