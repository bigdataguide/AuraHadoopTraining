package org.training.hadoop.hbase.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class UserOnlineBuy {
    public void process(Configuration conf, long uid) throws IOException {
        Connection connection = null;

        connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(OrderTableInformation.TABLE_NAME));

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(String.format("%08d", uid)));
        scan.setStopRow(Bytes.toBytes(String.format("%08d", uid + 1)));

        System.out.println("start: " + String.format("%08d", uid));
        System.out.println("stop: " + String.format("%08d", uid + 1));
        scan.addColumn(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_2), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_2_1_MERCHANT_ID));
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_2),
                Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_2_2_ACTION), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1"));
        scan.setFilter(filter);
        scan.setCaching(100);
        System.out.println("start to scan");
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            Cell cell = result.getColumnLatestCell(Bytes.toBytes(OrderTableInformation.FAMILY_NAME_2), Bytes.toBytes(OrderTableInformation.QUALIFIER_NAME_2_1_MERCHANT_ID));
            if (cell != null)
                System.out.println(Bytes.toString(cell.getValueArray(), cell.getValueOffset()));
        }
        table.close();
        connection.close();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("please offer uid");
            System.exit(-1);
        }
        UserOnlineBuy userOnlineBuy = new UserOnlineBuy();
        userOnlineBuy.process(HBaseConfiguration.create(), Long.valueOf(args[0]));
    }
}
