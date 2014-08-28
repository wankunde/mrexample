package com.wankun.mr.cache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * hadoop jar mrexample-1.0.0.jar com.wankun.mr.cache.CacheDemo /tmp/input3 /tmp/output3
 * 
 * 用法： job.addCacheFile(new URI("/tmp/cache#cachelink"));
 * 程序会先将/tmp/cache到各个tasktracker，在map或reduce中直接使用link名 cachelink 即可读取数据
 * cache的文件默认会到hdfs上找，也可以使用FileSystem标识文件位置，例如："file:///tmp/cache#cachelink"，
 * "hdfs:///tmp/cache#cachelink"
 * 
 * @author root
 *
 */
public class CacheDemo {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			System.out.println("Now, use the distributed cache and syslink");
			try {
				FileReader reader = new FileReader("cachelink");
				BufferedReader br = new BufferedReader(reader);
				String s = null;
				while ((s = br.readLine()) != null) {
					System.out.println(s);
				}
				br.close();
				reader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		// String cacheFile = "/tmp/cache#cache";
		// String cacheFile = "file:///tmp/cache#cachelink";
		String cacheFile = "hdfs:///tmp/cache#cachelink";

		// Path p = new
		// Path("/tmp/hadoop-0.20.2-capacity-scheduler.jar#hadoop-0.20.2-capacity-scheduler.jar");
		// DistributedCache.addArchiveToClassPath(p, conf);

		Job job = Job.getInstance(conf, "CacheDemo");
		job.addCacheFile(new URI(cacheFile));
		job.setJarByClass(CacheDemo.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// 再次运行前删除旧文件
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
