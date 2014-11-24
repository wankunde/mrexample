package com.wankun.mr.decompress;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author wankun
 * 
 * <pre>
 *  hadoop jar target/decompress-1.0.jar com.wankun.Decompress /tmp/tmp1/xx.tar.gz
 * </pre>
 */
public class Decompress {

	// map将输入中的value复制到输出数据的key上，并直接输出
	public static class Map extends Mapper<Object, Text, Text, NullWritable> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if(value!=null)
			context.write(value, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		conf.setInt("mapreduce.job.speculative.slownodethreshold", 0);
		conf.setInt("mapreduce.job.speculative.slowtaskthreshold", 0);
		conf.setInt("mapreduce.job.speculative.speculativecap", 0);

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 1) {
			System.out.println("Usage : Decompress [input] [input] .. ");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Decompress");
		job.setJarByClass(Decompress.class);
		job.setInputFormatClass(TarInputFormat.class);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		for (String input : otherArgs)
			FileInputFormat.addInputPath(job, new Path(input));

		Path out = new Path("/tmp/Decompress/" + new Path(otherArgs[0]).getName() + "_error");
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(out))
			hdfs.delete(out, true);
		FileOutputFormat.setOutputPath(job, out);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}