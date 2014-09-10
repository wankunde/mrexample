package com.wankun.hdfs.sequence;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * SequenceFile的写主要是通过SequenceFile.Writer进行写
 * 
 * @author wankun
 * 
 */
public class SequenceFileDemo {

	public static String uri = "/tmp/writedemo";
	// public static String uri = "hdfs://localhost.localdomain:8020/tmp/writedemo";
	// public static String uri = "hdfs://mycluster/tmp/writedemo";
	// public static String uri = "file:///tmp/writedemo";

	private static final String[] DATA = { "One, two, buckle my shoe", "Three, four, shut the door",
			"Five, six, pick up sticks", "Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

	public void writeSequenceFile() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			// 返回一个SequenceFile.Writer实例 需要数据流和path对象 将数据写入了path对象
			writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
					SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass()));

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

	public void readSequenceFile() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		SequenceFile.Reader reader = null;
		try {
			// 返回 SequenceFile.Reader 对象
			// Writer.filesystem(fs)
			// FileOption Writer.file(name)
			// InputStreamOption
			// StartOption
			// LengthOption
			// OnlyHeaderOption

			// StreamOption
			// BufferSizeOption
			// BlockSizeOption
			// ReplicationOption
			// KeyClassOption Writer.keyClass(keyClass)
			// ValueClassOption Writer.valueClass(valClass)
			// MetadataOption
			// ProgressableOption
			// CompressionOption Writer.compression(compressionType)

			reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
			// getKeyClass()获得Sequence中使用的类型
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			// ReflectionUtils.newInstace 得到常见的键值的实例
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);// 同上
			long position = reader.getPosition();
			while (reader.next(key, value)) { // next（）方法迭代读取记录 直到读完返回false
				String syncSeen = reader.syncSeen() ? "*" : "";// 替换特殊字符 同步
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition(); // beginning of next record
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}

	public static void main(String[] args) throws IOException {
		SequenceFileDemo demo = new SequenceFileDemo();
		demo.writeSequenceFile();
		demo.readSequenceFile();
	}
}