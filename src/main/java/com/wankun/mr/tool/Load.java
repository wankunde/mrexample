package com.wankun.mr.tool;

import java.util.Calendar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Load extends Configured implements Tool {
	private final static Logger logger = Logger.getLogger(Load.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Load(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		// 要处理哪天的数据
		final java.util.Date date;
		if (args.length >= 1) {
			String st = args[0];
			java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyyMMdd");
			try {
				date = sdf.parse(st);
			} catch (java.text.ParseException ex) {
				throw new RuntimeException("input format error," + st);
			}

		} else {
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -1);
			date = cal.getTime();
		}

		Job job = new Job(this.getConf(), "load_" + new java.text.SimpleDateFormat("MMdd").format(date));
		job.setJarByClass(Load.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(LoadMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(SearchLogProtobufWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

		// 输入目录，hadoop文件
		java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyyMMdd");
		String inputdir = "/log/raw/" + sdf.format(date) + "/*access.log*.gz";
		logger.info("inputdir = " + inputdir);

		/*
		 * 本地文件 java.text.SimpleDateFormat sdf = new
		 * java.text.SimpleDateFormat("yyyy/yyyyMM/yyyyMMdd"); String inputdir =
		 * "file:///opt/log/" + sdf.format(date) + "/*access.log*.gz";
		 * logger.info("inputdir = " + inputdir);
		 */

		// 输出目录
		final String outfilename = "/log/" + new java.text.SimpleDateFormat("yyyyMMdd").format(date) + "/access";
		logger.info("outfile dir = " + outfilename);
		Path outFile = new Path(outfilename);
		FileSystem.get(job.getConfiguration()).delete(outFile, true);

		FileInputFormat.addInputPath(job, new Path(inputdir));
		FileOutputFormat.setOutputPath(job, outFile);
		job.waitForCompletion(true);
		return 0;
	}

}