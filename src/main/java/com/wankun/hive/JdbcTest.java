package com.wankun.hive;

import java.sql.*;
import java.util.stream.IntStream;

/**
 * Created by wankun603 on 2018-03-29.
 */
// java -cp $(echo /usr/lib/hive/lib/*.jar | tr ' ' ':'):$(echo /usr/lib/hadoop/*.jar | tr ' ' ':'):./mrexample-1.0.0.jar com.wankun.hive.JdbcTest
public class JdbcTest {

  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static String url = "jdbc:hive2://26.6.0.32:10000/default";
  private static String username = "wankun_test";
  private static String password = "wankun_test";

  private static String sql = "select * from tmp.BULL_SL_RSI_D t\n" +
          "where dt='20180102'\n" +
          "and t.secu_ext_code='603288'";

  public static void main(String[] args)
          throws SQLException {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    IntStream.range(1,5)
            .forEach(i -> {
              new Thread(() -> {
                try {
                  Connection con = DriverManager.getConnection(url, username, password);
                  while (true) {
                    Statement stmt = con.createStatement();
                    ResultSet res = stmt.executeQuery(sql);
                    while (res.next()) {
                      System.out.println(res.next());
                    }
                    Thread.currentThread().sleep(1000l);
                  }
                } catch (SQLException e) {
                  e.printStackTrace();
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }).start();
            });
  }
}
