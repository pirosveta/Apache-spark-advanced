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

    public String getCancelled() {
        return cancelled;
    }

    public String getDelay() {
        return delay;
    }
    
    public String getDestAirportID() {
        return destAirportID;
    }

    public String getOriginAirportID() {
        return originAirportID;
    }
}
