package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ScanFromHBase {

  public static void scan(Configuration conf) throws IOException {
    Connection connection = null;

    connection = ConnectionFactory.createConnection(conf);

    Table table = connection.getTable(TableName.valueOf(TableInformation.TABLE_NAME));

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes("row1"));
    scan.setStopRow(Bytes.toBytes("row11"));
    scan.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1), Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_1));
    scan.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_2), Bytes.toBytes(TableInformation.QUALIFIER_NAME_2_2));
    scan.setCaching(100);
    ResultScanner results = table.getScanner(scan);

    for (Result result : results) {
      while (result.advance()) {
        System.out.println(result.current());
      }
    }
    table.close();
    connection.close();
  }

  public static void main(String args[]) throws IOException {
    ScanFromHBase.scan(TableInformation.getHBaseConfiguration());
  }
}
