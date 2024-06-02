package com.groupe_tpa_13;

import com.groupe_tpa_13.process.marketing.MarketingProcess;
import com.opencsv.exceptions.CsvException;

public class Main {
    public static void main(String[] args) throws CsvException {
        new MarketingProcess().insertData();
    }
}