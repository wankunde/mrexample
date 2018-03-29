package com.wankun.mr.cache;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.StringTokenizer;

/**
 * <pre>
 * DistributedCache 参数使用
 * 
 * hadoop jar target/mrexample-1.0.0.jar com.wankun.mr.cache.CacheDemo2 -files hdfs:///tmp/cache#cachelink  -libjars hdfs:///tmp/calutil-1.0.0.jar -archives hdfs:///tmp/myzip.zip,hdfs:///tmp/mytar.tar,hdfs:///tmp/mytargz.tar.gz,hdfs:///tmp/mytgz.tgz /tmp/input3 /tmp/output3
 * 
 * 这里引用的文件默认是在hadoop客户端的，提交任务时会将文件上传到hdfs上然后再分发到各个客户端
 * 
 * 
 * 运行时产生的临时文件如下所示：
 * 
 * .:
 * appcache  filecache
 * 
 * ./appcache:
 * application_1409121209604_0039
 * 
 * ./appcache/application_1409121209604_0039:
 * container_1409121209604_0039_01_000001  filecache  output  work
 * 
 * ./appcache/application_1409121209604_0039/container_1409121209604_0039_01_000001:
 * cachelink  calutil-1.0.0.jar  container_tokens  default_container_executor.sh  job.jar  jobSubmitDir  job.xml  launch_container.sh  mytargz.tar.gz  mytar.tar  mytgz.tgz  myzip.zip  tmp
 * 
 * ./appcache/application_1409121209604_0039/container_1409121209604_0039_01_000001/jobSubmitDir:
 * job.split  job.splitmetainfo
 * 
 * ./appcache/application_1409121209604_0039/container_1409121209604_0039_01_000001/tmp:
 * 
 * ./appcache/application_1409121209604_0039/filecache:
 * 10  11  12  13
 * 
 * ./appcache/application_1409121209604_0039/filecache/10:
 * job.jar
 * 
 * ./appcache/application_1409121209604_0039/filecache/10/job.jar:
 * job.jar
 * 
 * ./appcache/application_1409121209604_0039/filecache/11:
 * job.splitmetainfo
 * 
 * ./appcache/application_1409121209604_0039/filecache/12:
 * job.split
 * 
 * ./appcache/application_1409121209604_0039/filecache/13:
 * job.xml
 * 
 * ./appcache/application_1409121209604_0039/output:
 * attempt_1409121209604_0039_m_000000_0  attempt_1409121209604_0039_r_000000_0
 * 
 * ./appcache/application_1409121209604_0039/output/attempt_1409121209604_0039_m_000000_0:
 * file.out  file.out.index
 * 
 * ./appcache/application_1409121209604_0039/output/attempt_1409121209604_0039_r_000000_0:
 * 
 * ./appcache/application_1409121209604_0039/work:
 * 
 * ./filecache:
 * 
 * </pre>
 * 
 * @author root
 *
 */
public class CacheDemo2 {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private void printFileContent(String filename) {
			System.out.println("-------- cat file:" + filename
					+ " -------------");
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
//			Long res = CalUtil.add(3l, 7l);
//			System.out.println("cal result is : " + res);

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
		for (String arg : otherArgs)
			System.out.println("arg:" + arg);
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "CacheDemo");

		URI[] cacheFiles = job.getCacheFiles(); // 对应参数项-files
		Path[] archives = job.getArchiveClassPaths(); // -libjars
		URI[] cacheArchives = job.getCacheArchives();// 对应-archives

		if (cacheFiles != null)
			for (URI cf : cacheFiles)
				System.out.println("find cache File:" + cf);
		if (archives != null)
			for (Path cf : archives)
				System.out.println("find archive in calssPath:" + cf);
		if (cacheArchives != null)
			for (URI cf : cacheArchives)
				System.out.println("find cache Archive:" + cf);

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
