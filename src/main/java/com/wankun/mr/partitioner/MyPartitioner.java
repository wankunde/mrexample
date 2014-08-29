package com.wankun.mr.partitioner;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * 程序原型，每行数据由一个警告级别和一个所属模块组成，中间用竖线分割，
 * 现在统计各个警告级别的数据和所属模块的数据
 *示例数据：
 alert|dept1
 info|dept2
 notice|dept1
 alert|dept2

 输出结果两个文件
 file1：
 alert 2
 info 1
 notice 1

 file2:
 dept1 2
 dept2 2

 该程序使用的partition进行数据分组，不同类型的数据分在不同文件中

 */

public class MyPartitioner {
	/**
	 * 这里需要编写一个Mapper实现类，完成Map业务逻辑
	 * 
	 * @author wankun
	 */
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 没有配置 RecordReader，所以默认采用 line 的实现，
			// key 就是行号，value 就是行内容，
			String line = value.toString();
			String[] words = line.split("\\|");
			if (null == words || words.length != 2)
				return;
			context.write(new Text("logLevel::" + words[0]), one);
			context.write(new Text("dept::" + words[1]), one);
		}
	}

	/*这里通过判断key值将所有数据分布到两个reducer中*/
	public static class PartitionerClass extends Partitioner<Text, IntWritable> {
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			if (numPartitions >= 2)
				// Reduce 个数，判断 loglevel 还是dept的统计，分配到不同的 Reduce
				if (key.toString().startsWith("logLevel::"))
					return 0;
				else if (key.toString().startsWith("dept::"))
					return 1;
				else
					return 0;
			else
				return 0;
		}

	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int tmp = 0;
			for (IntWritable val : values) {
				tmp = tmp + val.get();
			}
			result.set(tmp);
			context.write(key, result);// 输出最后的汇总结果
		}
	}

	public static void main(String[] args)  throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MyPartitioner <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "MyPartitioner"); // Job名字
		job.setJarByClass(MyPartitioner.class); // 执行的类
		job.setMapperClass(MyMapper.class); // 指定Mapper类
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class); // 指定reducer类
		job.setPartitionerClass(PartitionerClass.class);
		job.setOutputKeyClass(Text.class); // 输出的key类型
		job.setOutputValueClass(IntWritable.class); // 输出value类型
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 从指定路径下读取文件
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); // 输出结果文件到指定路径下
		job.setNumReduceTasks(2);// 强制需要有两个 Reduce 来分别处理流量和次数的统计
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
