package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class WriteToHBase {

  public static void put(Configuration conf) throws IOException {

    //Connection to the cluster.
    Connection connection = null;
    //A lightweight handler for a specific table.
    Table table = null;

    try {
      //establish the connection to the cluster.
      connection = ConnectionFactory.createConnection(conf);
      //retrieve a handler to the target table
      table = connection.getTable(TableName.valueOf(TableInformation.TABLE_NAME));
      //describe the data
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1), Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_1), Bytes.toBytes(0));
      put.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1), Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_2), Bytes.toBytes(0));
      put.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_2), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_1), Bytes.toBytes(0));
      put.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_2), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_2), Bytes.toBytes(0));
      //send the data
      table.put(put);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      //close
      if (table != null) table.close();
      if (connection != null) connection.close();
    }
  }

  public static void asyncBatchPut(Configuration conf) throws IOException {
    //Connection to the cluster.
    Connection connection = null;
    //a async batch handler
    BufferedMutator bufferedMutator = null;

    //establish the connection to the cluster.
    try {
      connection = ConnectionFactory.createConnection(conf);
      bufferedMutator = connection.getBufferedMutator(TableName.valueOf(TableInformation.TABLE_NAME));
      //describe the data
      for (int i = 0; i < 1000; i++) {
        Put put = new Put(Bytes.toBytes("row" + String.format("%03d", i)));
        put.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1), Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_1), Bytes.toBytes(i));
        put.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1), Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_2), Bytes.toBytes(i));
        put.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_2), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_1), Bytes.toBytes(i));
        put.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_2), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_2), Bytes.toBytes(i));
        //add data to buffer
        bufferedMutator.mutate(put);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      //close
      if (bufferedMutator != null) bufferedMutator.close();
      if (connection != null) connection.close();
    }
  }

  public static void main(String[] args) throws IOException {
    //WriteToHBase.put(TableInformation.getHBaseConfiguration());
    WriteToHBase.asyncBatchPut(TableInformation.getHBaseConfiguration());
  }

}
