package com.groupe_tpa_13.process.marketing;

import oracle.kv.*;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.Table;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

public class MarketingProcess {

    KVStoreConfig config = new KVStoreConfig("kvstore", "localhost:5000");
    KVStore store = KVStoreFactory.getStore(config);
    TableAPI tableAPI = store.getTableAPI();
    String tableName = "marketing";

    public void insertData() throws CsvException {
        System.out.println("deleting table");
        dropTable();
        System.out.println("Creating table");
        createTable();
        //
        clearTable();
        try (InputStream inputStream = MarketingProcess.class.getResourceAsStream("/Marketing.csv")) {
            CSVReader reader = new CSVReader(new InputStreamReader(inputStream));
            reader.skip(1);
            List<String[]> csvData = reader.readAll();
            Table table = tableAPI.getTable(tableName);
            int id = 1;
            for (String[] entry : csvData) {
                Row row = table.createRow();
                row.put("id", id);
                row.put("age", Integer.parseInt(entry[0]));
                row.put("sexe", entry[1]);
                row.put("taux", Integer.parseInt(entry[2]));
                row.put("situationFamiliale", entry[3]);
                row.put("nbEnfantsACharge", Integer.parseInt(entry[4]));
                row.put("deuxiemeVoiture", Boolean.parseBoolean(entry[5]));

                tableAPI.put(row, null, null);
                id++;
            }
            reader.close();
            store.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTable() {
        String statement = " create table marketing("
                + "  id INTEGER,"
                + "  age  INTEGER ,"
                + "  sexe   STRING,"
                + "  taux  INTEGER,"
                + "  situationFamiliale  STRING,"
                + "  nbEnfantsAcharge     INTEGER,"
                + "  deuxiemeVoiture     Boolean,"
                + "  PRIMARY  KEY (id)) ";
        StatementResult result = null;
        try {
            result = store.executeSync(statement);
            showResult(result, statement);
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid statement:\n" + e.getMessage());
        } catch (FaultException e) {
            System.out.println("Statement couldn't be executed, please retry: " + e);
        }
    }

    public void dropTable() {
        String statement = "drop table if exists " + tableName;
        StatementResult result = null;
        try {
            result = store.executeSync(statement);
            showResult(result, statement);
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid statement:\n" + e.getMessage());
        } catch (FaultException e) {
            System.out.println("Statement couldn't be executed, please retry: " + e);
        }
    }

    public void clearTable() {
        Table table = tableAPI.getTable(tableName);
        PrimaryKey primaryKey = table.createPrimaryKey();
        TableIterator<Row> iterator = tableAPI.tableIterator(primaryKey, null, null);
        while (iterator.hasNext()) {
            Row row = iterator.next();
            tableAPI.delete(row.createPrimaryKey(), null, null);
        }
        iterator.close();
    }

    public void showResult(StatementResult result, String statement) {
        if (result.isSuccessful()) {
            System.out.println("Statement was successful:\n\t" +
                    statement);
            System.out.println("Results:\n\t" + result.getInfo());
        } else if (result.isCancelled()) {
            System.out.println("Statement was cancelled:\n\t" +
                    statement);
        } else {
            if (result.isDone()) {
                System.out.println("Statement failed:\n\t" + statement);
                System.out.println("Problem:\n\t" +
                        result.getErrorMessage());
            } else {

                System.out.println("Statement in progress:\n\t" +
                        statement);
                System.out.println("Status:\n\t" + result.getInfo());
            }
        }
    }
}
