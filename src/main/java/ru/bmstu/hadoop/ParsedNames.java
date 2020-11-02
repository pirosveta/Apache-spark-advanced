package ru.bmstu.hadoop;

import scala.Serializable;

public class ParsedNames implements Serializable {
    private static final String REGEX_FOR_REPLACE = "\"", REPLACE = "";
    private static final int 

    private String airportID, airportName;

    public ParsedNames(String[] data) {
        airportID = data[0].replaceAll(REGEX_FOR_REPLACE, REPLACE);
        airportName = data[1];
    }

    public String getAirportName() {
        return airportName;
    }

    public String getAirportID() {
        return airportID;
    }
}
