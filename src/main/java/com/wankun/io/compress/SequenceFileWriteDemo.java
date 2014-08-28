package com.wankun.io.compress;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * SequenceFile的写主要是通过SequenceFile.Writer进行写
 * @author wankun
 *
 */
public class SequenceFileWriteDemo {

	private static final String[] DATA = { "One, two, buckle my shoe", "Three, four, shut the door",
			"Five, six, pick up sticks", "Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

	public static void main(String[] args) throws IOException {
//		String uri = "hdfs://mycluster/tmp/writedemo";
		String uri = "file:///d:/tmp/writedemo";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			// 返回一个SequenceFile.Writer实例 需要数据流和path对象 将数据写入了path对象
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());

			for (int i = 0; i < 100; i++) {
				key.set(100 - i);
				value.set(DATA[i % DATA.length]);
				// getLength（）方法获取的是当前文件的读取位置在这个位置开始写
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				// 将每条记录追加到SequenceFile.Writer实例的末尾
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
}