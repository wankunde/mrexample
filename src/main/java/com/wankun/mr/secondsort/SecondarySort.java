package com.wankun.mr.secondsort;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 二次排序概念：首先按照第一字段排序，然后再对第一字段相同的行按照第二字段排序，注意不能破坏第一次排序的结果 。
 * 
 * @author 万昆
 */
public class SecondarySort {
	// map阶段的最后会对整个map的List进行分区，每个分区映射到一个reducer
	// numPartitions :
	// 这个应该是实际的reducer数，如果实际的reducer数比key计算出的分区少，且循环将数据分到各个reducer中
	// 本例中实际只有一个reduce，所以只有一个文件输出
	public static class FirstPartitioner extends HashPartitioner<Text, IntWritable> {
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			return (key.toString().split(":")[0].hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	// 每个分区内又调用job.setSortComparatorClass或者key的比较函数进行排序
	// 这个才是2次排序的关键
	public static class SortComparator extends WritableComparator {
		protected SortComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			// 同一个组内的数据，根据第二个字段进行比较
			if(w1.toString().split(":")[0].compareTo(w2.toString().split(":")[0])==0)
					return -w1.toString().split(":")[1].compareTo(w2.toString().split(":")[1]);
			// 不同组，返回组比较值
			return w1.toString().split(":")[0].compareTo(w2.toString().split(":")[0]);
		}
	}

	// 只要这个比较器比较的两个key相同，他们就属于同一个组.
	// 它们的value放在一个value迭代器，而这个迭代器的key使用属于同一个组的所有key的第一个key
	// 这个是一次排序的关键，可以达到数据分组输出的效果
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return w1.toString().split(":")[0].compareTo(w2.toString().split(":")[0]);
		}
	}

	// 自定义map
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final IntWritable intvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, intvalue);
		}
	}

	/**
	 * 根据第一列数据分组，同一组数据调用一次reduce，便于组内记录传递信息（必须排序），则value放在同一迭代器中，
	* @author wankun
	* @date 2014年8月26日
	* @version 1.0
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void setup(Context context) {
			context.getConfiguration();
			System.out.println("reduce");
		}

		// 虽然只有一个reduce进程进程，但是该进程内可以进行分组，同一个组内，执行一次该函数
		// values 在进行迭代的时候，会同步跟新对应的key值
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			context.write(new Text("-------------------------"), new IntWritable(1));
			for (IntWritable val : values) {
				// 虽然分在同一个组里，但是循环里每次输出的key都不相同(key看上去是个Text但实际也是一个list)
				context.write(key, val);
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "secondarysort");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		// 分区函数，在map阶段的最后，会先调用job.setPartitionerClass对所有map的结果List进行分区，每个分区映射到一个reducer。
		job.setPartitionerClass(FirstPartitioner.class);
		// 每个分区内又调用job.setSortComparatorClass设置的key比较函数类排序（map阶段）。
		job.setSortComparatorClass(SortComparator.class);
		// 分组函数，这个在reduce阶段，执行reduce方法之前使用
		// 只要这个比较器比较的两个key相同，他们就属于同一个组，它们的value放在一个value迭代器，而这个迭代器的key使用属于同一个组的所有key的第一个key。
		job.setGroupingComparatorClass(GroupingComparator.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("/tmp/input2"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/output2"));

		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

/*
 * 输入文件： hdfs://tmp/input2 
 * 内容：

1:3
3:2
2:3
1:2
2:1
1:1
3:1
2:2
3:3

 * 程序执行：
 * hadoop dfs -put input2 /tmp2
 * hadoop jar myhadoop.jar SecondarySort
 *
 */
