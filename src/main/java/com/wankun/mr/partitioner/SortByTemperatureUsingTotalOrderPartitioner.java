package com.wankun.mr.partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortByTemperatureUsingTotalOrderPartitioner {

	/* 这里通过判断key值将所有数据分布到两个reducer中 */
	public static class PartitionerClass extends Partitioner<IntPair, NullWritable> {
		public int getPartition(IntPair key, NullWritable value, int numPartitions) {
			return Math.abs(key.getFirst() * 127) % numPartitions;
		}
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(IntPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			return ip1.getFirst() - ip2.getFirst();
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(IntPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;

			return ip1.compareTo(ip2);
		}
	}

	// map将输入中的value化成IntWritable类型，作为输出的key
	public static class MyMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
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
	public static class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Data Deduplication");
		job.setJarByClass(SortByTemperatureUsingTotalOrderPartitioner.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		// 设置输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
//		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputKeyClass(IntWritable.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		SequenceFileOutputFormat.setCompressOutput(job, true);
		
		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path("/tmp/input4"));
		FileOutputFormat.setOutputPath(job, new Path("/tmp/output4"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

		
		//
		//
		// SequenceFileOutputFormat.setOutputCompressorClass(conf,
		// GzipCodec.class);
		// SequenceFileOutputFormat.setOutputCompressionType(conf,
		// CompressionType.BLOCK);
		// job.setPartitionerClass(TotalOrderPartitioner.class);
		// InputSampler.Sampler<IntWritable, Text> sampler = new
		// InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000,
		// 10);
		// Path input = FileInputFormat.getInputPaths(conf)[0];
		// input = input.makeQualified(input.getFileSystem(conf));
		// Path partitionFile = new Path(input, "_partitions");
		// TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		// InputSampler.writePartitionFile(conf, sampler);
		// // Add to DistributedCache
		// URI partitionUri = new URI(partitionFile.toString() +
		// "#_partitions");
		// DistributedCache.addCacheFile(partitionUri, conf);
		// DistributedCache.createSymlink(conf);
		// JobClient.runJob(conf);
	}

	/**
	 * 把第一列整数和第二列作为类的属性，并且实现WritableComparable接口
	 */
	public static class IntPair implements WritableComparable<IntPair> {
		private int first = 0;
		private int second = 0;

		public void set(int left, int right) {
			first = left;
			second = right;
		}

		public int getFirst() {
			return first;
		}

		public int getSecond() {
			return second;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			first = in.readInt();
			second = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(first);
			out.writeInt(second);
		}

		@Override
		public int hashCode() {
			return first + "".hashCode() + second + "".hashCode();
		}

		@Override
		public boolean equals(Object right) {
			if (right instanceof IntPair) {
				IntPair r = (IntPair) right;
				return r.first == first && r.second == second;
			} else {
				return false;
			}
		}

		// 这里的代码是关键，因为对key排序时，调用的就是这个compareTo方法
		@Override
		public int compareTo(IntPair o) {
			if (first != o.first) {
				return first - o.first;
			} else if (second != o.second) {
				return -(second - o.second);
			} else {
				return 0;
			}
		}
	}
}