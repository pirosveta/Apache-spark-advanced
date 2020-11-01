package ru.bmstu.hadoop;

import scala.Serializable;

public class ParsedData implements Serializable {
    private String value;
    private int row, column;

    public ParsedData(String value, int row, int column){
        this.value = value;
        this.row = row;
        this.column = column;
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