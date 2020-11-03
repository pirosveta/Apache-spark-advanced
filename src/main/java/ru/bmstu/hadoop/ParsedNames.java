package ru.bmstu.hadoop;

import scala.Serializable;

public class ParsedNames implements Serializable {
    private static final String REGEX_FOR_REPLACE = "\"", REPLACE = "";
    private static final int COLUMN_ID = 0, COLUMN_NAME = 1;

    private String airportID, airportName;

    public ParsedNames(String[] data) {
        airportID = data[COLUMN_ID].replaceAll(REGEX_FOR_REPLACE, REPLACE);
        airportName = data[COLUMN_NAME];
    }

    public String getAirportName() {
        return airportName;
    }

    public String getAirportID() {
        return airportID;
    }
}
