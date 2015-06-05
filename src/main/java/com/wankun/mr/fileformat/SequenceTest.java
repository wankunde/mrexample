package com.wankun.mr.fileformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * </pre>
 * Sequence File
 * 		存储记录为Record,Record由key和value组成
 * 		
 * </pre>
 * @author lenovo
 *
 */
public class SequenceTest {
	public static class Map extends Mapper<Object, Text, Text, Text> {
		private static Text line = new Text();

		public void map(Object key, Text value, Context context) throws IOException,
				InterruptedException {
			line = value;
			context.write(line, new Text(""));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			context.write(key, new Text(""));
		}
	}

	public static Job genjob(Configuration conf, String input, String output)
			throws ClassNotFoundException, IOException, InterruptedException {
		Job job = Job.getInstance(conf, "Sequence File Format");
		job.setJarByClass(SequenceTest.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Text -> SequenceFile
		FileInputFormat.addInputPath(job, new Path(input));
		SequenceFileOutputFormat.setOutputPath(job, new Path(output));
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		return job;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] ioArgs = new String[] { "/tmp/input1", "/tmp/output1", "/tmp/output2",
				"/tmp/output3" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.delete(new Path(ioArgs[1]), true);
		hdfs.delete(new Path(ioArgs[2]), true);
		hdfs.delete(new Path(ioArgs[3]), true);

		Job job = genjob(conf, otherArgs[0], otherArgs[1]);
		// 组合方式1：不压缩模式
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.NONE);
		job.waitForCompletion(true);

		job = genjob(conf, otherArgs[0], otherArgs[2]);
		// 组合方式2：record压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.RECORD);
		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		job.waitForCompletion(true);

		job = genjob(conf, otherArgs[0], otherArgs[3]);
		// 组合方式3：block压缩模式，并指定采用的压缩方式 ：默认、gzip压缩等
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		job.waitForCompletion(true);
	}
}
