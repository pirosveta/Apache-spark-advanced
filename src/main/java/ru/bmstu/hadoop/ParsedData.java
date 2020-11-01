package ru.bmstu.hadoop;

import scala.Serializable;

public class ParsedData implements Serializable {
    private String originAirportID, destAirportID, delay, cancelled;

    public ParsedData(String[] data){
        originAirportID = data[11];
        destAirportID = data[14];
        delay = data[18];
        cancelled = data[19];
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
