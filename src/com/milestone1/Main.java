package com.milestone1;

import java.time.Month;
import java.util.ArrayList;
import java.util.HashMap;

public class Main {

    public static void main(String[] args) {
        ColumnarMemory.loadIntoMemory(IOManager.readCsv());

        HashMap<String, HashMap<Month, ArrayList<RowEntry>>> queryMemoryResult1 = QueryManager.getMinMaxValuesFromYearAndStation(2004, "Paya Lebar");
        HashMap<String, HashMap<Month, ArrayList<RowEntry>>> queryMemoryResult2 = QueryManager.getMinMaxValuesFromYearAndStation(2014, "Paya Lebar");

        HashMap<String, HashMap<Month, ArrayList<RowEntry>>> queryDiskResult1 = QueryManager.getMinMaxValuesFromYearAndStationDisk(2004, "Paya Lebar");
        HashMap<String, HashMap<Month, ArrayList<RowEntry>>> queryDiskResult2 = QueryManager.getMinMaxValuesFromYearAndStationDisk(2014, "Paya Lebar");

        IOManager.createOutputFilesWithHeaders();

        IOManager.processOutputResults(queryMemoryResult1, "Paya Lebar", "ScanResult");
        IOManager.processOutputResults(queryMemoryResult2, "Paya Lebar", "ScanResult");

        IOManager.processOutputResults(queryDiskResult1, "Paya Lebar", "ScanResult (Disk)");
        IOManager.processOutputResults(queryDiskResult2, "Paya Lebar", "ScanResult (Disk)");

        IOManager.printExeSuccessMessage();
    }
}
