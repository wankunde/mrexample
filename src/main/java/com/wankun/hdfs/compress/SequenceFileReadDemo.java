package com.wankun.hdfs.compress;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

// vv SequenceFileReadDemo
public class SequenceFileReadDemo {

	public static void main(String[] args) throws IOException {
		// String uri = "hdfs://mycluster/tmp/writedemo";
		String uri = "file:///tmp/writedemo";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		SequenceFile.Reader reader = null;
		try {
			// 返回 SequenceFile.Reader 对象
			reader = new SequenceFile.Reader(fs, path, conf);
//			reader = new SequenceFile.Reader(fs, path, conf);
			// getKeyClass()获得Sequence中使用的类型
			Writable key = (Writable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			 // ReflectionUtils.newInstace 得到常见的键值的实例
			Writable value = (Writable)	ReflectionUtils.newInstance(reader.getValueClass(), conf);// 同上
			long position = reader.getPosition();
			while (reader.next(key, value)) { // next（）方法迭代读取记录 直到读完返回false
				String syncSeen = reader.syncSeen() ? "*" : "";// 替换特殊字符 同步
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key,
						value);
				position = reader.getPosition(); // beginning of next record
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
