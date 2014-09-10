package com.wankun.mr.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *  1. 多输入源
 *  2. 测试表数据Join
 * 
 * 
 * @author wankun
 *
 */
public class MultiInput {
	// map将输入中的value复制到输出数据的key上，并直接输出
	public static class Map extends Mapper<Object, Text, Text, Text> {
		private static final Log LOG = LogFactory.getLog(Map.class);

		// 实现map函数
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			String name = split.getPath().getName();
			String[] fields = value.toString().split(" ");
			if (name.equals("user"))
				context.write(new Text(fields[0]),
						new Text("user-" + fields[1]));
			if (name.equals("sex"))
				context.write(new Text(fields[0]), new Text("sex-" + fields[1]));
		}
	}

	// reduce将输入中的key复制到输出数据的key上，并直接输出
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> users = new ArrayList<String>();
			List<String> sexs = new ArrayList<String>();

			for (Text s : values) {
				String str = s.toString();
				if (str.startsWith("user-"))
					users.add(str.substring(5));
				if (str.startsWith("sex-"))
					sexs.add(str.substring(4));
			}
			for (String user : users)
				for (String sex : sexs)
					context.write(new Text(user), new Text(sex));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] ioArgs = { "/tmp/input7/", "/tmp/output4/" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.exit(2);
		}

		// 当文件是200M左右的时候，可以适当将split.size扩大到256m，这样map数会减半
		// conf.setLong("mapred.min.split.size", 268435456);

		FileSystem.get(conf).delete(new Path(ioArgs[1]), true);
		Job job = Job.getInstance(conf, "multi input files ");
		job.setJarByClass(MultiInput.class);
		// 设置Map、Combine和Reduce处理类
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// 设置输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path("/tmp/user/"));
		FileInputFormat.addInputPath(job, new Path("/tmp/sex/"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/output4/"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}