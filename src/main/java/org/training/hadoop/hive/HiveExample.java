package org.training.hadoop.hive;

import java.sql.*;

/**
 * Created by qianxi.zhang on 6/7/17.
 */
public class HiveExample {

  private static String driverName = "org.apache.hive.jdbc.HiveDriver";

  public void process() throws SQLException {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    Connection con = DriverManager.getConnection(
        "jdbc:hive2://bigdata:10000/default", "bigdata", "bigdata");

    Statement stmt = con.createStatement();
    long startTime = System.currentTimeMillis();
    String sql = "show tables";
    ResultSet res = stmt.executeQuery(sql);
    int count = 0;
    while (res.next()) {
      count++;
      System.out.println(res.getString(1));
    }
    long stopTime = System.currentTimeMillis();
    System.out.println("time: " + (stopTime - startTime) + ", count : " + count);
  }

  public static void main(String[] args) throws SQLException {
    HiveExample example = new HiveExample();
    example.process();
  }
}
