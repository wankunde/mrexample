import java.io.IOException;
import java.util.StringTokenizer;

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
import org.apache.log4j.Logger;

/*
 * 增加partitioner,combiner的模块的使用
 * partitioner实现map的数据传输到指定的reduce中
 * combiner : 每一个map可能会产生大量的输出，combiner的作用就是在map端对输出先做一次合并，以减少传输到reducer的数据量。
 * combiner的输入输出类型必须和mapper的输出以及reducer的输入类型一致
 * 输入数据文件
 aa
 bb
 aa
 dd
 ff
 rr
 ee
 aa
 kk
 jj
 hh
 uu
 ii
 tt
 rr
 tt
 oo
 uu

 * @author wankun
 *
 */
public class WordCount2 {

	protected static Logger logger = Logger.getLogger(WordCount2.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			logger.info("进入map" + key);
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	// 自定义数据分到reduce方式
	private static class MyPartitioner extends Partitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			logger.info("进入partitioner" + key);
			System.out.println("current key is " + key);
			if (key.toString().compareTo("aa") >= 0 && key.toString().compareTo("kk") <= 0) {
				return 0;
			} else {
				return 1;
			}
		}
	}

	// 自定义本地聚合方法
	private static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			logger.info("进入partitioner" + key);
			System.out.println("current key is " + key);

			int sum = 0;
			for (IntWritable value : values) {
				sum += Integer.parseInt(value.toString());
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			logger.info("进入reduce" + key);
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
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		logger.info("设置集群内容");
		job.setNumReduceTasks(2);

		job.setJarByClass(WordCount2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		logger.info("启动集群");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}