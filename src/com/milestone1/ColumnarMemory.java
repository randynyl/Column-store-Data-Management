package com.milestone1;

import java.util.ArrayList;

/**
 * Memory to store data columns from CSV file
 */
public class ColumnarMemory {
    // no need to store id column in memory since it is not used during querying
    private static ArrayList<String> timestampCol;
    private static ArrayList<String> stationCol;
    private static ArrayList<Float> temperatureCol;
    private static ArrayList<Float> humidityCol;

    private static final int TIMESTAMP_INDEX = 1;
    private static final int STATION_INDEX = 2;
    private static final int TEMPERATURE_INDEX = 3;
    private static final int HUMIDITY_INDEX = 4;

    public static void loadIntoMemory(ArrayList<ArrayList<?>> data) {
        timestampCol = (ArrayList<String>) data.get(TIMESTAMP_INDEX);
        stationCol = (ArrayList<String>) data.get(STATION_INDEX);
        temperatureCol = (ArrayList<Float>) data.get(TEMPERATURE_INDEX);
        humidityCol = (ArrayList<Float>) data.get(HUMIDITY_INDEX);
    }

    public static ArrayList<String> getTimestampCol() {
        return timestampCol;
    }

    public static ArrayList<String> getStationCol() {
        return stationCol;
    }

    public static ArrayList<Float> getTemperatureCol() {
        return temperatureCol;
    }

    public static ArrayList<Float> getHumidityCol() {
        return humidityCol;
    }
}
