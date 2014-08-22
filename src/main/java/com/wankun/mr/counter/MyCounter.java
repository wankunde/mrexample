package com.wankun.mr.counter;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 自定义计数器，详细见map类
 * 
 * @author wankun
 * 
 */
public class MyCounter {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//其实从2.0开始，org.apache.hadoop.mapreduce.Counter从1.0版本的class改为interface
			//在使用的时候需要使用相应版本的库进行编译
			// 这里添加自定义计数器
			final Counter counter = context.getCounter("Error Rows", "Empty Row");
			if (StringUtils.isEmpty(value.toString())) {
				counter.increment(1L);
			}

			// 没有配置 RecordReader，所以默认采用 line 的实现，key 就是行号，value 就是行内容，
			StringTokenizer itr = new StringTokenizer(value.toString()); // 针对每一条记录进行数据切分
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				// Map数据，源数据被Map成 <word,1> 这样的一个List传递给reduce
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		// 经过shuffle过程，把map数据变为相同key对于的list
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int sum = 0;
			// 针对相对key的值做业务逻辑
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result); // 输出计算结果
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		// 多次运行前需要清理数据
		final FileSystem fileSystem = FileSystem.get(new URI("hdfs://mycluster"), conf);
		fileSystem.delete(new Path(otherArgs[1]), true);

		Job job = new Job(conf, "word count"); // Job名字
		job.setJarByClass(MyCounter.class); // 执行的类
		job.setMapperClass(TokenizerMapper.class); // 指定Mapper类
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class); // 指定reducer类
		job.setOutputKeyClass(Text.class); // 输出的key类型
		job.setOutputValueClass(IntWritable.class); // 输出value类型
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 从指定路径下读取文件
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); // 输出结果文件到指定路径下
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}