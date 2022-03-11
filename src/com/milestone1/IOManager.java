package com.milestone1;
import com.opencsv.CSVReader;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Month;
import java.util.*;
import java.util.stream.Stream;

/**
 * Handles reading and writing of CSV files.
 */
public class IOManager {
    private static ArrayList<String> idCol = new ArrayList<>();
    private static ArrayList<String> timestampCol = new ArrayList<>();
    private static ArrayList<String> stationCol = new ArrayList<>();
    private static ArrayList<Float> temperatureCol = new ArrayList<>();
    private static ArrayList<Float> humidityCol = new ArrayList<>();
    private static ArrayList<ArrayList<?>> data = new ArrayList<>();

    public static ArrayList<ArrayList<?>> readCsv() {
        CSVReader reader = null;
        try {
            //parsing a CSV file into CSVReader class constructor
            reader = new CSVReader(new FileReader("SingaporeWeather.csv"));
            System.out.println("Reading CSV file..");
            String[] nextLine;
            //reads one line at a time
            int nextCol = 0;
            reader.readNext();
            while ((nextLine = reader.readNext()) != null) {
                for(String token : nextLine) {
                    switch(nextCol) {
                        case 0:
                            idCol.add(token);
                            nextCol++;
                            break;
                        case 1:
                            timestampCol.add(token);
                            nextCol++;
                            break;
                        case 2:
                            stationCol.add(token);
                            nextCol++;
                            break;
                        case 3:
                            temperatureCol.add(Parser.stringToFloat(token));
                            nextCol++;
                            break;
                        case 4:
                            humidityCol.add(Parser.stringToFloat(token));
                            nextCol = 0;
                            break;
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        Collections.addAll(data, idCol, timestampCol, stationCol, temperatureCol, humidityCol);

        // saving columns into disk for task 2
        saveColumnAsTxt("timestamp", timestampCol);
        saveColumnAsTxt("station", stationCol);
        saveColumnAsTxt("temperature", temperatureCol);
        saveColumnAsTxt("humidity", humidityCol);

        return data;
    }

    /**
     * Creates blank CSV file with result headers.
     */
    public static void createOutputFilesWithHeaders() {
        try {
            FileWriter writer1 = new FileWriter("ScanResult.csv");
            writer1.append("Date,Station,Category,Value\n");
            writer1.flush();

            FileWriter writer2 = new FileWriter("ScanResult (Disk).csv");
            writer2.append("Date,Station,Category,Value\n");
            writer2.flush();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Appends output rows to CSV file
     * @param results HashMap obtained from query results.
     * @param station Location to obtain data of.
     */
    public static void processOutputResults(HashMap<String, HashMap<Month, ArrayList<RowEntry>>> results, String station, String fileName) {
        String filePath = fileName + ".csv";
        try {
            FileWriter writer = new FileWriter(filePath, true);
            System.out.println("Writing query results to " + filePath);
            for (Map.Entry<String, HashMap<Month, ArrayList<RowEntry>>> entry : results.entrySet()) {
                String category = entry.getKey();
                HashMap<Month, ArrayList<RowEntry>> monthlyResults = entry.getValue();
                for (ArrayList<RowEntry> monthlyOutputRows : monthlyResults.values()) {
                    for (int i=0; i<monthlyOutputRows.size(); i++) {
                        String value = monthlyOutputRows.get(i).getValue().toString();
                        String date = monthlyOutputRows.get(i).getDate();
                        writer.append(date + ",");
                        writer.append(station + ",");
                        writer.append(category + ",");
                        writer.append(value + "\n");
                    }
                }
            }
            writer.flush();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }


    }

    /**
     * Stores column of data as txt file on disk.
     * @param fileName name of columnar file to be created
     * @param columnArr column array of values to be stored
     */
    public static void saveColumnAsTxt(String fileName, ArrayList<?> columnArr) {
        String filePath = fileName + ".txt";
        File fout = new File(filePath);
        try (FileOutputStream fos = new FileOutputStream(fout); BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));) {
            for (Object item : columnArr) {
                bw.write(item.toString());
                bw.newLine();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * Retrieves the entire column of data from the file on disk.
     * @param fileName file to read
     * @return List of values of the column
     */
    public static List<String> getWholeColumnFromDisk(String fileName) {
        String filePath = fileName + ".txt";
        List<String> column = Collections.emptyList();
        try {
            column = Files.readAllLines(Paths.get(filePath));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return column;
    }

    /**
     * Retrieves the value within the file at specified position.
     * @param fileName file to read
     * @param pos position of value
     * @return String value from disk file
     */
    public static String getValueFromDisk(String fileName, int pos) {
        String filePath = fileName + ".txt";
        String value = "";
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            value = lines.skip(pos).findFirst().get();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return value;
    }

    public static void printExeSuccessMessage() {
        System.out.println("Queries executed successfully, terminating program.");
    }
}
