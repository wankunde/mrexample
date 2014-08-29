package com.wankun.mr.partitioner;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * <pre>
 *  全局排序
 *  InputSampler.RandomSampler：采样数据
 *  TotalOrderPartitioner：用来全局paitition分区
 *  
 * hadoop jar target/mrexample-1.0.0.jar com.wankun.mr.partitioner.TotalSortMR /tmp/input2 /tmp/output2 /tmp/_partition 2
 * </pre>
 * 
 * @author root
 *
 */
public class TotalSortMR {

	public static int runTotalSortJob(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: TotalSortMR <in> <out> <partitonfile> <reduceNum>");
			System.exit(2);
		}

		Path inputPath = new Path(otherArgs[0]);
		Path outputPath = new Path(otherArgs[1]);
		Path partitionFile = new Path(otherArgs[2]);
		int reduceNumber = Integer.parseInt(otherArgs[3]);
		for (String arg : otherArgs) {
			System.out.println("args--->" + arg);
		}
		FileSystem.get(conf).delete(outputPath, true);

		// 这个必须要在job前将参数映射到conf中
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		// RandomSampler第一个参数表示key会被选中的概率，第二个参数是一个选取samples数，第三个参数是最大读取input splits数
		RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 2, 3);

		Job job = Job.getInstance(conf, "Total-Sort");
		job.setJarByClass(TotalSortMR.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setNumReduceTasks(reduceNumber);
		// partitioner class设置成TotalOrderPartitioner
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.addCacheArchive(new URI(partitionFile.toString()));
		// 写partition file到mapreduce.totalorderpartitioner.path
		InputSampler.writePartitionFile(job, sampler);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(runTotalSortJob(args));
	}
}