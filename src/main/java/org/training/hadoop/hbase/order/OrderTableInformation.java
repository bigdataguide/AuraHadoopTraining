package org.training.hadoop.hbase.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.training.hadoop.hbase.TableInformation;

import java.io.IOException;

public class OrderTableInformation {
    public static final String TABLE_NAME = "order";
    public static final String FAMILY_NAME_1 = "f";
    public static final String FAMILY_NAME_2 = "o";

    public static final String QUALIFIER_NAME_1_1_MERCHANT_ID = "merchant_id";
    public static final String QUALIFIER_NAME_1_2_COUPON_ID = "coupon_id";
    public static final String QUALIFIER_NAME_1_3_DISCOUNT_RATE = "discount_rate";
    public static final String QUALIFIER_NAME_1_4_DISTANCE = "distance";
    public static final String QUALIFIER_NAME_1_5_DATE_RECEIVED = "date_received";

    public static final String QUALIFIER_NAME_2_1_MERCHANT_ID = "merchant_id";
    public static final String QUALIFIER_NAME_2_2_ACTION = "action";
    public static final String QUALIFIER_NAME_2_3_COUPON_ID = "coupon_id";
    public static final String QUALIFIER_NAME_2_4_DISCOUNT_RATE = "discount_rate";
    public static final String QUALIFIER_NAME_2_5_DATE_RECEIVED = "date_received";

    public static final String NULL = "null";

    public static Configuration getHBaseConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",
                "bigdata");
        conf.set("zookeeper.znode.parent", "/hbase");

        return conf;
    }

    public static void createTable(Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            HColumnDescriptor columnDescriptor_1 = new HColumnDescriptor(Bytes.toBytes(FAMILY_NAME_1));
            HColumnDescriptor columnDescriptor_2 = new HColumnDescriptor(Bytes.toBytes(FAMILY_NAME_2));
            tableDescriptor.addFamily(columnDescriptor_1);
            tableDescriptor.addFamily(columnDescriptor_2);
            admin.createTable(tableDescriptor);
        }
    }

    public static void deleteTable(Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
            if (!admin.isTableDisabled(TableName.valueOf(TABLE_NAME))) {
                admin.disableTable(TableName.valueOf(TABLE_NAME));
            }
            admin.deleteTable(TableName.valueOf(TABLE_NAME));
        }
    }

    public static void main(String args[]) throws IOException {
        OrderTableInformation.createTable(OrderTableInformation.getHBaseConfiguration());
    }

}
