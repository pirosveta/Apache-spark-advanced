package ru.bmstu.hadoop;

import scala.Serializable;

public class ParsedNames implements Serializable {
    private String airportID, airportName;

    public ParsedNames(String[] data) {
        airportID = data[0].replaceAll("\"", "");
        airportName = data[1];
    }

    public String getAirportName() {
        return airportName;
    }

    public String getAirportID() {
        return airportName;
    }
}
