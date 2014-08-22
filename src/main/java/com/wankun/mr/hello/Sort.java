package com.wankun.mr.hello;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * hadoop排序
 * 这里使用的的时候map完数据后传递给reduce时，shuffle阶段根据指定的setSortComparatorClass类进行数据排序
 * ，在reduce阶段接收排序好的数据处理即可
 * 
 * hadoop默认是按照key值进行排序的，如果key为封装int的IntWritable类型，那么MapReduce按照数字大小对key排序，
 * 如果key为封装为String的Text类型，那么MapReduce按照字典顺序对字符串排序。 这里我们自定义排序规则，根据int的大小倒排序
 * 
 * @author wankun
 * 
 */
public class Sort {

	// map将输入中的value化成IntWritable类型，作为输出的key
	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> {
		private static IntWritable data = new IntWritable();

		// 实现map函数
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			data.set(Integer.parseInt(line));
			context.write(data, new IntWritable(1));
		}

	}

	// reduce将输入中的key复制到输出数据的key上，
	// 然后根据输入的value-list中元素的个数决定key的输出次数
	// 用全局linenum来代表key的位次
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private static IntWritable linenum = new IntWritable(1);

		// 实现reduce函数
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			for (IntWritable val : values) {
				context.write(linenum, key);
				linenum = new IntWritable(linenum.get() + 1);
			}

		}

	}

	// 这里是排序的实现，可以自定义自己的排序算法
	public static class SortComparator extends WritableComparator {
		protected SortComparator() {
			super(IntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable int1 = (IntWritable) w1;
			IntWritable int2 = (IntWritable) w2;
			return -int1.compareTo(int2);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		final FileSystem fileSystem = FileSystem.get(new URI("hdfs://mycluster"), conf);
		fileSystem.delete(new Path("/tmp/sort_out"), true);

		String[] ioArgs = new String[] { "/tmp/sort_in", "/tmp/sort_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.exit(2);
		}

		Job job = new Job(conf, "Data Sort");
		job.setJarByClass(Sort.class);

		// 设置Map和Reduce处理类
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setSortComparatorClass(SortComparator.class);

		// 设置输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}