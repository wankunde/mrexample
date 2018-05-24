package com.wankun.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Created by WANKUN603 on 2018-05-24.
 */
public class Utiltools {

  public static String getStackTraceString(Throwable tr) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    tr.printStackTrace(pw);
    pw.flush();
    return sw.toString();
  }

  public static void main(String[] args) {
    new Utiltools().p1();
  }

  private void p1() {
    p2();
  }

  private void p2() {
    System.out.println(getStackTraceString(new Throwable()));
  }


}
