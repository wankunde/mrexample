package com.wankun.mr.sort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

/**
 * 
 * <p>
 * 对排序进行单元测试，map数据在reduce之前会对key进行排序
 * </p>
 * 
 * @author wankun
 * @date 2014年8月26日
 * @version 1.0
 */
public class SortTest {
	MapReduceDriver<Object, Text, IntWritable, IntWritable, IntWritable, IntWritable> mapReduceDriver;


	@Test
	public void testMapperReducer() throws IOException {
		Sort.Map mapper = new Sort.Map();
		Sort.Reduce reducer = new Sort.Reduce();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		mapReduceDriver.withInput(new LongWritable(), new Text("15"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("9"));
		mapReduceDriver.withInput(new LongWritable(), new Text("41"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("25"));
		mapReduceDriver.withOutput(new IntWritable(1), new IntWritable(2));
		mapReduceDriver.withOutput(new IntWritable(2), new IntWritable(2));
		mapReduceDriver.withOutput(new IntWritable(3), new IntWritable(9));
		mapReduceDriver.withOutput(new IntWritable(4), new IntWritable(15));
		mapReduceDriver.withOutput(new IntWritable(5), new IntWritable(25));
		mapReduceDriver.withOutput(new IntWritable(6), new IntWritable(41));
		mapReduceDriver.runTest();
	}
	
	/**
	 * 测试key的倒排序
	 * @throws IOException
	 */
	@Test
	public void testComparator() throws IOException {
		Sort.Map mapper = new Sort.Map();
		Sort.Reduce reducer = new Sort.Reduce();
		Sort.SortComparator comparator = new Sort.SortComparator();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
//		mapReduceDriver.setKeyComparator(comparator);
		mapReduceDriver.setKeyOrderComparator(comparator);
		mapReduceDriver.withInput(new LongWritable(), new Text("15"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("9"));
		mapReduceDriver.withInput(new LongWritable(), new Text("41"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("25"));
		mapReduceDriver.withOutput(new IntWritable(1), new IntWritable(41));
		mapReduceDriver.withOutput(new IntWritable(2), new IntWritable(25));
		mapReduceDriver.withOutput(new IntWritable(3), new IntWritable(15));
		mapReduceDriver.withOutput(new IntWritable(4), new IntWritable(9));
		mapReduceDriver.withOutput(new IntWritable(5), new IntWritable(2));
		mapReduceDriver.withOutput(new IntWritable(6), new IntWritable(2));
		mapReduceDriver.runTest();
	}
}
