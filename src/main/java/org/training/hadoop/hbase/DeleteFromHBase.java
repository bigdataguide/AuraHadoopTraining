package org.training.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DeleteFromHBase {
  public static void delete(Configuration conf) throws IOException {
    Connection connection = null;

    connection = ConnectionFactory.createConnection(conf);

    Table table = connection.getTable(TableName.valueOf(TableInformation.TABLE_NAME));

    Delete delete = new Delete(Bytes.toBytes("row1"));
    delete.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1), Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_1));
    delete.addColumn(Bytes.toBytes(TableInformation.FAMILY_NAME_1), Bytes.toBytes(TableInformation.QUALIFIER_NAME_1_2), 1000000);
    table.delete(delete);

    table.close();
    connection.close();
  }

  public static void main(String[] args) throws IOException {
    DeleteFromHBase.delete(TableInformation.getHBaseConfiguration());
  }
}

