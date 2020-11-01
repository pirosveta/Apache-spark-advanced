package ru.bmstu.hadoop;

public class ParsedNames {
    private String airportID, airportName;

    public ParsedNames(String[] data) {
        airportID = data[0].replaceAll("\"", "");
        airportName = data[1];
    }

    public String getName() {
        return airportName;
    }
}
