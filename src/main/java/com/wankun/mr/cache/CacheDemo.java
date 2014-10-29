package com.wankun.mr.cache;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.wankun.calutil.CalUtil;

/**
 * <pre>
 * hadoop jar target/mrexample-1.0.0.jar com.wankun.mr.cache.CacheDemo /tmp/input3 /tmp/output3
 * 
 * DistributedCache 使用
 * 
 * 1. 缓存文件
 * 		用法一：API调用
 * 
 * job.addCacheFile(new URI("/tmp/cache#cachelink"));
 * 程序会先将hdfs上的/tmp/cache到各个tasktracker，在map或reduce中直接使用link名 cachelink 即可读取数据
 * cache的文件默认会到hdfs上找，也可以使用FileSystem标识文件位置，例如："file:///tmp/cache#cachelink"，
 * "hdfs:///tmp/cache#cachelink"
 * 
 * 
 * 在使用文件的时候，可以直接使用文件名，也可以使用文件的软链接，但是不能使用文件的绝对路径访问，这样会找不到文件
 * 归档文件（zip、tar 和tgz/tar.gz文件）
 * 
 * 		用法二：参数调用
 * </pre>
 * 
 * @author root
 *
 */
public class CacheDemo {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private void printFileContent(String filename) {
			System.out.println("-------- cat file:" + filename + " -------------");
			try {
				System.out.println(new File(filename).getCanonicalPath());
				List<String> lines = Files.readLines(new File(filename),
						Charsets.UTF_8);
				for (String line : lines)
					System.out.println(line);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		protected void setup(Context context) throws IOException,
				InterruptedException {
			// test distribute file
			System.out.println("Now, use the distributed cache and syslink");
			printFileContent("cachelink");

			// test distribute jar
			Long res = CalUtil.add(3l, 7l);
			System.out.println("cal result is : " + res);

			// test archive read
			printFileContent("myzip.zip/zipfile");
			printFileContent("mytar.tar/tarfile");
			printFileContent("mytargz.tar.gz/targz");
			printFileContent("mytgz.tgz/targz");
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

		Job job = Job.getInstance(conf, "CacheDemo");

		// String cacheFile = "/tmp/cache#cache";
		// String cacheFile = "file:///tmp/cache#cachelink";
		String cacheFile = "hdfs:///tmp/cache#cachelink";
		job.addCacheFile(new URI(cacheFile));

		Path archive = new Path("/tmp/calutil-1.0.0.jar");
		job.addArchiveToClassPath(archive);

		// 归档文件（zip、tar 和tgz/tar.gz文件）
//		job.addCacheArchive(new URI("/tmp/myzip.zip#myzip.zip"));
//		job.addCacheArchive(new URI("/tmp/mytar.tar#mytar.tar"));
//		job.addCacheArchive(new URI("/tmp/mytargz.tar.gz#mytargz.tar.gz"));
//		job.addCacheArchive(new URI("/tmp/mytgz.tgz#mytgz.tgz"));
		
		job.addCacheArchive(new URI("/tmp/myzip.zip"));
		job.addCacheArchive(new URI("/tmp/mytar.tar"));
		job.addCacheArchive(new URI("/tmp/mytargz.tar.gz"));
		job.addCacheArchive(new URI("/tmp/mytgz.tgz"));

		URI[] cacheFiles = job.getCacheFiles(); // 对应参数项-files
		Path[] archives = job.getArchiveClassPaths();
		URI[] cacheArchives = job.getCacheArchives();// 对应-archives

		for(URI cf:cacheFiles)
			System.out.println("find cache File:"+cf);
		for(Path cf:archives)
			System.out.println("find archive in calssPath:"+cf);
		for(URI cf:cacheArchives)
			System.out.println("find cache Archive:"+cf);

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
