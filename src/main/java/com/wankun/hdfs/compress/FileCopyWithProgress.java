package com.wankun.hdfs.compress;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {
	public static void main(String[] args) throws Exception {
		String localSrc = "/tmp/input";
		String dst = "/tmp/output";

		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		// 建立一个本地的InputStream
		Configuration conf = new Configuration();
		// 程序默认会加载配置文件，如果加载不到，Fileystem会是本地文件系统
		// conf.addResource("core-site.xml");
		// conf.addResource("hdfs-site.xml");
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				System.out.print(".");
			}// 通过实例创建管道 create方法返回的是FSDataOutputStream对象
		});

		System.out.println("begin copy ");
		IOUtils.copyBytes(in, out, 4096, true);// 通过管道进行写入
		System.out.println("end copy ");
		out.close();
	}
}