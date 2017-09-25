package org.training.hadoop.hive;

import java.sql.*;

/**
 * Created by qianxi.zhang on 6/7/17.
 */
public class HiveExample2 {

  private static String driverName = "org.apache.hive.jdbc.HiveDriver";

  public void process() throws SQLException {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    Connection con = DriverManager.getConnection(
        "jdbc:hive2://192.168.92.137:10000/default", "qushy", "qushy");

    Statement stmt = con.createStatement();
    stmt.execute("use dbtest1");

    long startTime = System.currentTimeMillis();
    String sql = "select province, sum(price) as totalPrice " +
            "from record join user_dimension on record.uid=user_dimension.uid " +
            "group by province " +
            "order by totalPrice desc";
    ResultSet res = stmt.executeQuery(sql);
    int count = 0;
    while (res.next()) {
      count++;
      System.out.println(res.getString(1)+"--"+res.getString(2));
    }
    long stopTime = System.currentTimeMillis();
    System.out.println("time: " + (stopTime - startTime) + ", count : " + count);
  }

  public static void main(String[] args) throws SQLException {
    HiveExample2 example = new HiveExample2();
    example.process();
  }
}
