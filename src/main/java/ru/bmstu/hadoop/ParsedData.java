package ru.bmstu.hadoop;

import scala.Serializable;

public class ParsedData implements Serializable {
    private String ;

    public ParsedData(String[] data){

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
