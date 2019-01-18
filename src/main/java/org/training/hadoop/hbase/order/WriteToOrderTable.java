package org.training.hadoop.hbase.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

import static org.training.hadoop.hbase.order.OrderTableInformation.NULL;

public class WriteToOrderTable {
    private String offline_file_path;
    private String online_file_path;

    private BufferedReader offline_reader;
    private BufferedReader online_reader;

    private Configuration conf;
    Connection connection;
    Table table;

    public WriteToOrderTable(String offline_file_path, String online_file_path, Configuration conf) {
        this.offline_file_path = offline_file_path;
        this.online_file_path = online_file_path;
        this.conf = conf;
    }

    public void init() throws IOException {
        //Connection to the cluster.
        //establish the connection to the cluster.
        connection = ConnectionFactory.createConnection(conf);
        //retrieve a handler to the target table
        table = connection.getTable(TableName.valueOf(OrderTableInformation.TABLE_NAME));
    }

    public void initOfflineReader() throws FileNotFoundException {
        File file = new File(offline_file_path);
        offline_reader = new BufferedReader(new FileReader(file));
    }

    public void initOnlineReader() throws FileNotFoundException {
        File file = new File(online_file_path);
        online_reader = new BufferedReader(new FileReader(file));
    }

    public void putOfflineData(String[] fields) throws IOException {
        //bypass dirty data
        String row_key = String.format("%08d", Long.valueOf(fields[0])) + "_";
        if (!fields[6].equals(NULL)) {
            row_key += fields[6];
        }

        Put put = new Put(Bytes.toBytes(row_key));
        if (!fields[1].equals(NULL)) {
            put.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_1), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_1_1_MERCHANT_ID), Bytes.toBytes(fields[1]));
        }
        if (!fields[2].equals(NULL)) {
            put.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_1), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_1_2_COUPON_ID), Bytes.toBytes(fields[2]));
        }
        if (!fields[3].equals(NULL)) {
            put.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_1), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_1_3_DISCOUNT_RATE), Bytes.toBytes(fields[3]));
        }
        if (!fields[4].equals(NULL)) {
            put.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_1), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_1_4_DISTANCE), Bytes.toBytes(fields[4]));
        }
        if (!fields[5].equals(NULL)) {
            put.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_1), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_1_5_DATE_RECEIVED), Bytes.toBytes(fields[5]));
        }
        //send the data
        table.put(put);
    }

    public void putOnlineData(String[] fields) throws IOException {
        //bypass dirty data
        if (fields.length != 7) return;

        String row_key = String.format("%08d", Long.valueOf(fields[0])) + "_";
        if (!fields[6].equals(NULL)) {
            row_key += fields[6];
        }
        Put put = new Put(Bytes.toBytes(row_key));
        if (!fields[1].equals(NULL)) {
            put.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_2), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_2_1_MERCHANT_ID), Bytes.toBytes(fields[1]));
        }
        if (!fields[2].equals(NULL)) {
            put.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_2), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_2_2_ACTION), Bytes.toBytes(fields[2]));
        }
        if (!fields[3].equals(NULL)) {
            put.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_2), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_2_3_COUPON_ID), Bytes.toBytes(fields[3]));
        }
        if (!fields[4].equals(NULL)) {
            put.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_2), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_2_4_DISCOUNT_RATE), Bytes.toBytes(fields[4]));
        }
        if (!fields[5].equals(NULL)) {
            put.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_2), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_2_5_DATE_RECEIVED), Bytes.toBytes(fields[5]));
        }
        //send the data
        table.put(put);
    }


    public String[] NextOrderFields(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null;
        } else {
            return line.split(",");
        }
    }

    public void writeOfflineData() throws IOException {
        System.out.println("start to write offline data");
        initOfflineReader();
        String[] fields;
        while ((fields = NextOrderFields(offline_reader)) != null) {
            putOfflineData(fields);
        }
        offline_reader.close();
        System.out.println("done");
    }

    public void writeOnlineData() throws IOException {
        System.out.println("start to write online data");
        initOnlineReader();
        String[] fields;
        while ((fields = NextOrderFields(online_reader)) != null) {
            putOnlineData(fields);
        }
        online_reader.close();
        System.out.println("done");
    }

    public void close() throws IOException {
        if (table != null) table.close();
        if (connection != null) connection.close();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("please offer offline file and online file");
            System.exit(-1);
        }
        String offline_file = args[0];
        String online_file = args[1];
        WriteToOrderTable writeToOrderTable = new WriteToOrderTable(offline_file, online_file, HBaseConfiguration.create());
        writeToOrderTable.init();
        writeToOrderTable.writeOfflineData();
        writeToOrderTable.writeOnlineData();
        writeToOrderTable.close();
    }

}
