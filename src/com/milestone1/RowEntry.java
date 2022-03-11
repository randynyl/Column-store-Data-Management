package com.milestone1;

/**
 * Data object to store date and category values.
 */
public class RowEntry {
    private String date;
    private Float value;

    public RowEntry(String date, Float value) {
        this.date = date;
        this.value = value;
    }

    public String getDate() {
        return date;
    }

    public Float getValue() {
        return value;
    }
}
