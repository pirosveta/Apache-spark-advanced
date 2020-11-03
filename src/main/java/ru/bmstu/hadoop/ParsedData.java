package ru.bmstu.hadoop;

import scala.Serializable;

public class ParsedData implements Serializable {
    private final int COLUMN_ORIGIN_ID = 11, COLUMN_DEST_ID = 14, COLUMN_DELAY = 18, COLUMN_CANCELLED = 19;

    private String originAirportID, destAirportID, delay, cancelled;

    public ParsedData(String[] data){
        originAirportID = data[COLUMN_ORIGIN_ID];
        destAirportID = data[COLUMN_DEST_ID];
        delay = data[COLUMN_DELAY];
        cancelled = data[COLUMN_CANCELLED];
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
