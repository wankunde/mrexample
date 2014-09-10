package com.wankun.hdfs.sequence;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class MapFileDemo {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Path dir = new Path("file:///tmp/");
		Path mapFile = new Path("mapfile");

		// Writer内部类用于文件的写操作,假设Key和Value都为Text类型
		MapFile.Writer writer = new MapFile.Writer(conf, dir, SequenceFile.Writer.file(mapFile),
				MapFile.Writer.keyClass(Text.class), MapFile.Writer.valueClass(Text.class));

		// 通过writer向文档中写入记录
		writer.append(new Text("key"), new Text("value"));
		IOUtils.closeStream(writer);// 关闭write流

		// Reader内部类用于文件的读取操作
		MapFile.Reader reader = new MapFile.Reader(dir, conf, SequenceFile.Reader.file(mapFile));
		// 通过reader从文档中读取记录
		Text key = new Text();
		Text value = new Text();
		while (reader.next(key, value)) {
			System.out.println(key);
			System.out.println(value);
		}
		IOUtils.closeStream(reader);// 关闭read流

	}
}
