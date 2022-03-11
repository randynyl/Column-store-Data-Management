package com.milestone1;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Parser {

    public static float stringToFloat(String string) {
        try {
            return Float.parseFloat(string);
        } catch (NumberFormatException e) {
            return Float.MAX_VALUE;
        }
    }

    /**
     * Reads in the timestamp string and returns a Calendar object with date information.
     * @param timestamp string from CSV data.
     * @return Calendar object with date details.
     */
    public static Calendar timestampStringToDate(String timestamp) {
        Calendar cal = Calendar.getInstance();
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(("yyyy-MM-dd HH:mm"));
            cal.setTime(formatter.parse(timestamp));
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            cal.setTime(new Date(Long.MIN_VALUE));
        }
        return cal;
    }

    /**
     * converts the Calendar object back to date string to output to CSV
      */
    public static String dateToDateString(Calendar cal) {
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1; // Calendar.MONTH enum uses zero indexing
        int date = cal.get(Calendar.DATE);
        return year + "-" + month + "-" + date;
    }
}
