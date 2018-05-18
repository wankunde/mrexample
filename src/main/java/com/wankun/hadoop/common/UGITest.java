package com.wankun.hadoop.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * impersonate another user
 * <p>
 * UserGroupInformation : 可以伪装为其他用户在Hadoop中平台进行操作
 * 通过在 core-site.xml 配置可以伪装的用户 acl（hosts，groups），用户即可以伪装为其他用户进行操作
 * <p>
 * 参考：https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/Superusers.html
 * http://dongxicheng.org/mapreduce-nextgen/hadoop-secure-impersonation/
 */
public class UGITest {
  public static void main(String[] args) throws IOException, InterruptedException {
    System.out.println("1. ugi current user:" + UserGroupInformation.getCurrentUser());
    System.out.println("1. ugi login user:" + UserGroupInformation.getLoginUser());
    String user = "hadoop";
    UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws Exception {
        System.out.println("2. ugi current user:" + UserGroupInformation.getCurrentUser());
        System.out.println("2. ugi login user:" + UserGroupInformation.getLoginUser());
        FileSystem fs = FileSystem.get(new Configuration());
        fs.create(new org.apache.hadoop.fs.Path("/tmp/test_file"));
        return null;
      }
    });
  }
}
