package com.wankun.mr.join;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 表自身关联，通过关系 child - parent 找出 grandchild - grandparent 的关系
 * 
 * @author wankun
 *
 */
public class STjoin {
	public static int time = 0;

	/*
	 * 
	 * map将输出分割child和parent，然后正序输出一次作为右表， 反序输出一次作为左表，需要注意的是在输出的value中必须
	 * 加上左右表的区别标识。
	 */
	public static class Map extends Mapper<Object, Text, Text, Text> {
		// 实现map函数
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String childname = new String();// 孩子名称
			String parentname = new String();// 父母名称
			String relationtype = new String();// 左右表标识
			// 输入的一行预处理文本
			StringTokenizer itr = new StringTokenizer(value.toString());
			String[] values = new String[2];
			int i = 0;
			while (itr.hasMoreTokens()) {
				values[i] = itr.nextToken();
				i++;
			}
			if (values[0].compareTo("child") != 0) {
				childname = values[0];
				parentname = values[1];
				// 输出左表
				relationtype = "1";
				context.write(new Text(values[1]), new Text(relationtype + "+" + childname + "+" + parentname));
				// 输出右表
				relationtype = "2";
				context.write(new Text(values[0]), new Text(relationtype + "+" + childname + "+" + parentname));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// 实现reduce函数
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 输出表头
			if (0 == time) {
				context.write(new Text("grandchild"), new Text("grandparent"));
				time++;
			}
			List<String> grandchild = new ArrayList<String>();
			List<String> grandparent = new ArrayList<String>();

			Iterator ite = values.iterator();
			while (ite.hasNext()) {
				String record = ite.next().toString();
				int len = record.length();
				if (0 == len) {
					continue;
				}
				// 使用正则表达式取数据，这里要注意转义
				String[] ss = record.split("\\+");
				char relationtype = record.charAt(0);
				String childname = ss[1];
				String parentname = ss[2];

				// 左表，取出child放入grandchildren
				if ('1' == relationtype) {
					grandchild.add(childname);
				}
				// 右表，取出parent放入grandparent
				if ('2' == relationtype) {
					grandparent.add(parentname);
				}
			}
			// grandchild和grandparent数组求笛卡尔儿积
			if (grandchild.size() > 0 && grandparent.size() > 0) {
				for (String gc : grandchild)
					for (String gp : grandparent)
						context.write(new Text(gc), new Text(gp));

			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] ioArgs = new String[] { "/tmp/STjoin_in", "/tmp/STjoin_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.exit(2);
		}

		final FileSystem fileSystem = FileSystem.get(new URI("hdfs://mycluster"), conf);
		fileSystem.delete(new Path("/tmp/STjoin_out"), true);

		Job job = new Job(conf, "Single Table Join");
		job.setJarByClass(STjoin.class);
		// 设置Map和Reduce处理类
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}