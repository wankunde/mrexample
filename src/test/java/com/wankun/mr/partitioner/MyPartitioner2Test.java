package com.wankun.mr.partitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * <p> partitioner暂时不支持单元测试 </p>
 * 
 * @author wankun
 * @date 2014年8月25日
 * @version 1.0
 */
public class MyPartitioner2Test {
	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		MyPartitioner2.MyMapper mapper = new MyPartitioner2.MyMapper();
		MyPartitioner2.MyReducer reducer = new MyPartitioner2.MyReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("alert|dept1"));
		mapDriver.withInput(new LongWritable(), new Text("info|dept2"));
		mapDriver.withInput(new LongWritable(), new Text("notice|dept1"));
		mapDriver.withInput(new LongWritable(), new Text("alert|dept2"));

		mapDriver.withOutput(new Text("logLevel::" + "alert"), new IntWritable(1));
		mapDriver.withOutput(new Text("dept::" + "dept1"), new IntWritable(1));
		mapDriver.withOutput(new Text("logLevel::" + "info"), new IntWritable(1));
		mapDriver.withOutput(new Text("dept::" + "dept2"), new IntWritable(1));
		mapDriver.withOutput(new Text("logLevel::" + "notice"), new IntWritable(1));
		mapDriver.withOutput(new Text("dept::" + "dept1"), new IntWritable(1));
		mapDriver.withOutput(new Text("logLevel::" + "alert"), new IntWritable(1));
		mapDriver.withOutput(new Text("dept::" + "dept2"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>(Arrays.asList(new IntWritable(1), new IntWritable(1)));
		reduceDriver.withInput(new Text("logLevel::" + "alert"), values);
		reduceDriver.withOutput(new Text("logLevel::" + "alert"), new IntWritable(2));
		reduceDriver.runTest();
	}

	@Test
	public void testMapperReducer() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("alert|dept1"));
		mapReduceDriver.withOutput(new Text("dept::" + "dept1"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("logLevel::" + "alert"), new IntWritable(1));
		mapReduceDriver.runTest();
	}
}
