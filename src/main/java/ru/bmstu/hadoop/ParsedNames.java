package ru.bmstu.hadoop;

import scala.Serializable;

public class ParsedNames implements Serializable {
    private String airportID, airportName;

    public ParsedNames(String[] data) {
        airportID = data[0];
        airportName = data[1];
    }

    public String getAirportID() {
        return airportID;
    }

    public String getAirportName() {
        return airportName;
    }
}
