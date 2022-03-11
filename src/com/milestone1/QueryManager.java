package com.milestone1;

import java.time.Month;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

/**
 * Handles processing of query
 */
public class QueryManager {

    /**
     * Uses a column store search logic to filter rows satisfying query conditions.
     * @param year period of query.
     * @param station location of query.
     * @return HashMap containing monthly maximum and minimum temperature and humuidity results.
     */
    public static HashMap<String, HashMap<Month, ArrayList<RowEntry>>> getMinMaxValuesFromYearAndStation(int year, String station) {
        System.out.println("Processing query for year: " + year + " at station: " + station + " with data from memory");
        // filter year column first due to lower selectivity
        ArrayList<Integer> yearPos = filterYearPos(year, ColumnarMemory.getTimestampCol());
        ArrayList<Integer> yearAndStationPos = filterStationPos(station, ColumnarMemory.getStationCol(), yearPos);
        HashMap<String, HashMap<Month, ArrayList<RowEntry>>> result;
        result = getMonthlyMinMaxTemp(ColumnarMemory.getTemperatureCol(), ColumnarMemory.getTimestampCol(), yearAndStationPos);
        result.putAll(getMonthlyMinMaxHumidity(ColumnarMemory.getHumidityCol(), ColumnarMemory.getTimestampCol(), yearAndStationPos));

        return result;
    }

    public static HashMap<String, HashMap<Month, ArrayList<RowEntry>>> getMinMaxValuesFromYearAndStationDisk(int year, String station) {
        System.out.println("Processing query for year: " + year + " at station: " + station + " with data from disk");
        // filter year column first due to lower selectivity
        ArrayList<Integer> yearPos = filterYearPosDisk(year);
        ArrayList<Integer> yearAndStationPos = filterStationPosDisk(station, yearPos);
        HashMap<String, HashMap<Month, ArrayList<RowEntry>>> result;
        result = getMonthlyMinMaxTempDisk(yearAndStationPos);
        result.putAll(getMonthlyMinMaxHumidityDisk(yearAndStationPos));

        return result;
    }

    /**
     * Iterates through the timestamp column in memory and returns an array of row indexes with the corresponding year.
     */
    public static ArrayList<Integer> filterYearPos(int year, ArrayList<String> timestampCol) {
        ArrayList<Integer> yearPos = new ArrayList<>();
        for (int i=0; i<timestampCol.size(); i++) {
            Calendar cal = Parser.timestampStringToDate(timestampCol.get(i));
            if (cal.get(Calendar.YEAR) == year) {
                yearPos.add(i);
            }
        }
        return yearPos;
    }

    public static ArrayList<Integer> filterYearPosDisk(int year) {
        ArrayList<Integer> yearPos = new ArrayList<>();
        List<String> timestampCol = IOManager.getWholeColumnFromDisk("timestamp");
        for (int i=0; i<timestampCol.size(); i++) {
            Calendar cal = Parser.timestampStringToDate(timestampCol.get(i));
            if (cal.get(Calendar.YEAR) == year) {
                yearPos.add(i);
            }
        }
        return yearPos;
    }

    /**
     * Iterates through the station column from filtered year positions and returns an array of row indexes with the corresponding station.
     */
    public static ArrayList<Integer> filterStationPos(String station, ArrayList<String> stationCol, ArrayList<Integer> pos) {
        ArrayList<Integer> yearAndStationPos = new ArrayList<>();
        for (int i=0; i<pos.size(); i++) {
            if (stationCol.get(pos.get(i)).equals(station)) {
                yearAndStationPos.add(pos.get(i));
            }
        }
        return yearAndStationPos;
    }

    public static ArrayList<Integer> filterStationPosDisk(String stationToQuery, ArrayList<Integer> pos) {
        ArrayList<Integer> yearAndStationPos = new ArrayList<>();
        for (int i=0; i<pos.size(); i++) {
            String stationFromDisk = IOManager.getValueFromDisk("station", pos.get(i));
            if (stationFromDisk.equals(stationToQuery)) {
                yearAndStationPos.add(pos.get(i));
            }
        }
        return yearAndStationPos;
    }

    /**
     * Iterates through the temperature column from filtered year and station positions to find max and min monthly temperatures
     * @param temperatureCol in memory
     * @param timestampCol in memory
     * @param pos filtered positions by year and station
     * @return Hashmap containing max and min temperatures for each month.
     */
    public static HashMap<String, HashMap<Month, ArrayList<RowEntry>>> getMonthlyMinMaxTemp(ArrayList<Float> temperatureCol,
                                                                                         ArrayList<String> timestampCol,
                                                                                         ArrayList<Integer> pos) {

        HashMap<String, HashMap<Month, ArrayList<RowEntry>>> maxMinTempResult = new HashMap<>();
        HashMap<Month, ArrayList<RowEntry>> monthlyMaxTempResult = new HashMap<>();
        HashMap<Month, ArrayList<RowEntry>> monthlyMinTempResult = new HashMap<>();

        for (Month month : Month.values()) {
            monthlyMaxTempResult.put(month, new ArrayList<>());
            monthlyMinTempResult.put(month, new ArrayList<>());
        }

        for (int i=0; i<pos.size(); i++) {
            Calendar cal = Parser.timestampStringToDate(timestampCol.get(pos.get(i)));
            Float temperatureValue = temperatureCol.get(pos.get(i));
            evalEntryValue(monthlyMaxTempResult, monthlyMinTempResult, cal, temperatureValue);
        }

        maxMinTempResult.put("Max Temperature", monthlyMaxTempResult);
        maxMinTempResult.put("Min Temperature", monthlyMinTempResult);
        return maxMinTempResult;
    }

    public static HashMap<String, HashMap<Month, ArrayList<RowEntry>>> getMonthlyMinMaxTempDisk(ArrayList<Integer> pos) {

        HashMap<String, HashMap<Month, ArrayList<RowEntry>>> maxMinTempResult = new HashMap<>();
        HashMap<Month, ArrayList<RowEntry>> monthlyMaxTempResult = new HashMap<>();
        HashMap<Month, ArrayList<RowEntry>> monthlyMinTempResult = new HashMap<>();

        for (Month month : Month.values()) {
            monthlyMaxTempResult.put(month, new ArrayList<>());
            monthlyMinTempResult.put(month, new ArrayList<>());
        }

        for (int i=0; i<pos.size(); i++) {
            String timestampFromDisk = IOManager.getValueFromDisk("timestamp", pos.get(i));
            String temperatureFromDisk = IOManager.getValueFromDisk("temperature", pos.get(i));

            Calendar cal = Parser.timestampStringToDate(timestampFromDisk);
            Float temperatureValue = Float.parseFloat(temperatureFromDisk);

            evalEntryValue(monthlyMaxTempResult, monthlyMinTempResult, cal, temperatureValue);
        }
        maxMinTempResult.put("Max Temperature", monthlyMaxTempResult);
        maxMinTempResult.put("Min Temperature", monthlyMinTempResult);
        return maxMinTempResult;
    }

    /**
     * Iterates through the humidity column from filtered year and station positions to find max and min monthly humidity values.
     * @param humidityCol
     * @param timestampCol
     * @param pos filtered positions by year and station
     * @return Hashmap containing max and min humidity values for each month.
     */
    public static HashMap<String, HashMap<Month, ArrayList<RowEntry>>> getMonthlyMinMaxHumidity(ArrayList<Float> humidityCol,
                                                                                                ArrayList<String> timestampCol,
                                                                                                ArrayList<Integer> pos) {

        HashMap<String, HashMap<Month, ArrayList<RowEntry>>> maxMinHumidityResult = new HashMap<>();
        HashMap<Month, ArrayList<RowEntry>> monthlyMaxHumidityResult = new HashMap<>();
        HashMap<Month, ArrayList<RowEntry>> monthlyMinHumidityResult = new HashMap<>();

        for (Month month : Month.values()) {
            monthlyMaxHumidityResult.put(month, new ArrayList<>());
            monthlyMinHumidityResult.put(month, new ArrayList<>());
        }

        for (int i=0; i<pos.size(); i++) {
            Calendar cal = Parser.timestampStringToDate(timestampCol.get(pos.get(i)));
            Float humidityValue = humidityCol.get(pos.get(i));
            evalEntryValue(monthlyMaxHumidityResult, monthlyMinHumidityResult, cal, humidityValue);
        }

        maxMinHumidityResult.put("Max Humidity", monthlyMaxHumidityResult);
        maxMinHumidityResult.put("Min Humidity", monthlyMinHumidityResult);
        return maxMinHumidityResult;
    }

    public static HashMap<String, HashMap<Month, ArrayList<RowEntry>>> getMonthlyMinMaxHumidityDisk(ArrayList<Integer> pos) {

        HashMap<String, HashMap<Month, ArrayList<RowEntry>>> maxMinHumidityResult = new HashMap<>();
        HashMap<Month, ArrayList<RowEntry>> monthlyMaxHumidityResult = new HashMap<>();
        HashMap<Month, ArrayList<RowEntry>> monthlyMinHumidityResult = new HashMap<>();

        for (Month month : Month.values()) {
            monthlyMaxHumidityResult.put(month, new ArrayList<>());
            monthlyMinHumidityResult.put(month, new ArrayList<>());
        }

        for (int i=0; i<pos.size(); i++) {
            String timestampFromDisk = IOManager.getValueFromDisk("timestamp", pos.get(i));
            String humidityFromDisk = IOManager.getValueFromDisk("humidity", pos.get(i));

            Calendar cal = Parser.timestampStringToDate(timestampFromDisk);
            Float humidityValue = Float.parseFloat(humidityFromDisk);

            evalEntryValue(monthlyMaxHumidityResult, monthlyMinHumidityResult, cal, humidityValue);
        }

        maxMinHumidityResult.put("Max Humidity", monthlyMaxHumidityResult);
        maxMinHumidityResult.put("Min Humidity", monthlyMinHumidityResult);
        return maxMinHumidityResult;
    }

    /**
     * function to evaluate whether the current value in the entry should replace the current max or min values when iterating through the necessary positions of the column.
     */
    private static void evalEntryValue(HashMap<Month, ArrayList<RowEntry>> monthlyMaxResult, HashMap<Month, ArrayList<RowEntry>> monthlyMinResult, Calendar cal, Float value) {
        int month = cal.get(Calendar.MONTH);
        if (value == Float.MAX_VALUE) {
            return;
        }
        switch (month) {
            case Calendar.JANUARY:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.JANUARY).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.JANUARY).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.JANUARY).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.JANUARY).get(0).getValue()) {
                    monthlyMaxResult.get(Month.JANUARY).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.JANUARY).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.JANUARY).get(0).getValue()) {
                    monthlyMinResult.get(Month.JANUARY).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.JANUARY).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.JANUARY).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.JANUARY).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.JANUARY).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.JANUARY).add(newEntry);
                }
                break;
            case Calendar.FEBRUARY:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.FEBRUARY).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.FEBRUARY).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.FEBRUARY).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.FEBRUARY).get(0).getValue()) {
                    monthlyMaxResult.get(Month.FEBRUARY).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.FEBRUARY).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.FEBRUARY).get(0).getValue()) {
                    monthlyMinResult.get(Month.FEBRUARY).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.FEBRUARY).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.FEBRUARY).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.FEBRUARY).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.FEBRUARY).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.FEBRUARY).add(newEntry);
                }
                break;
            case Calendar.MARCH:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.MARCH).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.MARCH).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.MARCH).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.MARCH).get(0).getValue()) {
                    monthlyMaxResult.get(Month.MARCH).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.MARCH).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.MARCH).get(0).getValue()) {
                    monthlyMinResult.get(Month.MARCH).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.MARCH).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.MARCH).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.MARCH).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.MARCH).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.MARCH).add(newEntry);
                }
                break;
            case Calendar.APRIL:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.APRIL).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.APRIL).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.APRIL).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.APRIL).get(0).getValue()) {
                    monthlyMaxResult.get(Month.APRIL).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.APRIL).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.APRIL).get(0).getValue()) {
                    monthlyMinResult.get(Month.APRIL).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.APRIL).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.APRIL).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.APRIL).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.APRIL).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.APRIL).add(newEntry);
                }
                break;
            case Calendar.MAY:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.MAY).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.MAY).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.MAY).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.MAY).get(0).getValue()) {
                    monthlyMaxResult.get(Month.MAY).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.MAY).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.MAY).get(0).getValue()) {
                    monthlyMinResult.get(Month.MAY).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.MAY).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.MAY).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.MAY).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.MAY).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.MAY).add(newEntry);
                }
                break;
            case Calendar.JUNE:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.JUNE).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.JUNE).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.JUNE).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.JUNE).get(0).getValue()) {
                    monthlyMaxResult.get(Month.JUNE).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.JUNE).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.JUNE).get(0).getValue()) {
                    monthlyMinResult.get(Month.JUNE).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.JUNE).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.JUNE).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.JUNE).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.JUNE).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.JUNE).add(newEntry);
                }
                break;
            case Calendar.JULY:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.JULY).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.JULY).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.JULY).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.JULY).get(0).getValue()) {
                    monthlyMaxResult.get(Month.JULY).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.JULY).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.JULY).get(0).getValue()) {
                    monthlyMinResult.get(Month.JULY).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.JULY).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.JULY).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.JULY).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.JULY).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.JULY).add(newEntry);
                }
                break;
            case Calendar.AUGUST:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.AUGUST).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.AUGUST).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.AUGUST).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.AUGUST).get(0).getValue()) {
                    monthlyMaxResult.get(Month.AUGUST).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.AUGUST).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.AUGUST).get(0).getValue()) {
                    monthlyMinResult.get(Month.AUGUST).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.AUGUST).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.AUGUST).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.AUGUST).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.AUGUST).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.AUGUST).add(newEntry);
                }
                break;
            case Calendar.SEPTEMBER:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.SEPTEMBER).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.SEPTEMBER).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.SEPTEMBER).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.SEPTEMBER).get(0).getValue()) {
                    monthlyMaxResult.get(Month.SEPTEMBER).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.SEPTEMBER).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.SEPTEMBER).get(0).getValue()) {
                    monthlyMinResult.get(Month.SEPTEMBER).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.SEPTEMBER).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.SEPTEMBER).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.SEPTEMBER).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.SEPTEMBER).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.SEPTEMBER).add(newEntry);
                }
                break;
            case Calendar.OCTOBER:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.OCTOBER).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.OCTOBER).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.OCTOBER).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.OCTOBER).get(0).getValue()) {
                    monthlyMaxResult.get(Month.OCTOBER).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.OCTOBER).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.OCTOBER).get(0).getValue()) {
                    monthlyMinResult.get(Month.OCTOBER).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.OCTOBER).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.OCTOBER).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.OCTOBER).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.OCTOBER).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.OCTOBER).add(newEntry);
                }
                break;
            case Calendar.NOVEMBER:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.NOVEMBER).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.NOVEMBER).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.NOVEMBER).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.NOVEMBER).get(0).getValue()) {
                    monthlyMaxResult.get(Month.NOVEMBER).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.NOVEMBER).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.NOVEMBER).get(0).getValue()) {
                    monthlyMinResult.get(Month.NOVEMBER).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.NOVEMBER).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.NOVEMBER).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.NOVEMBER).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.NOVEMBER).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.NOVEMBER).add(newEntry);
                }
                break;
            case Calendar.DECEMBER:
                // first row of respective month with available data, so we set as max or min values
                if (monthlyMaxResult.get(Month.DECEMBER).size() == 0) {
                    String date = Parser.dateToDateString(cal);
                    RowEntry firstMonthlyEntry = new RowEntry(date, value);
                    monthlyMaxResult.get(Month.DECEMBER).add(firstMonthlyEntry);
                    monthlyMinResult.get(Month.DECEMBER).add(firstMonthlyEntry);
                } // new max value found
                else if (value > monthlyMaxResult.get(Month.DECEMBER).get(0).getValue()) {
                    monthlyMaxResult.get(Month.DECEMBER).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.DECEMBER).add(newEntry);
                } // new min value found
                else if (value < monthlyMinResult.get(Month.DECEMBER).get(0).getValue()) {
                    monthlyMinResult.get(Month.DECEMBER).clear();
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.DECEMBER).add(newEntry);
                } // same max value on a different date found
                else if (value == monthlyMaxResult.get(Month.DECEMBER).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMaxResult.get(Month.DECEMBER).add(newEntry);
                } // same min value on a different date found
                else if (value == monthlyMinResult.get(Month.DECEMBER).get(0).getValue()) {
                    RowEntry newEntry = new RowEntry(Parser.dateToDateString(cal), value);
                    monthlyMinResult.get(Month.DECEMBER).add(newEntry);
                }
                break;
        }
    }


}

