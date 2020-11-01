package ru.bmstu.hadoop;

import scala.Serializable;

public class ParsedData implements Serializable {
    private String originAirportID, destAirportID, delay, cancelled;

    public ParsedData(String[] data){
        originAirportID = data[];
        destAirportID = data[];
        delay = data[];
        cancelled = data[];
    };

    public String getValue() {
        return value;
    }

    public int getRow() {
        return row;
    }

    public int getColumn() {
        return column;
    }
}
