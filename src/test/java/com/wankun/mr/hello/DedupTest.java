package com.wankun.mr.hello;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class DedupTest {
	MapDriver<Object, Text, Text, Text>  mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		Dedup.Map mapper = new Dedup.Map();
		Dedup.Reduce reducer = new Dedup.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("key1"));
		mapDriver.withInput(new LongWritable(), new Text("key1"));
		mapDriver.withInput(new LongWritable(), new Text("key2"));
		mapDriver.withInput(new LongWritable(), new Text("key2"));
		mapDriver.withInput(new LongWritable(), new Text("key2"));
		mapDriver.withInput(new LongWritable(), new Text("key3"));
		mapDriver.withOutput(new Text("key1"), new Text(""));
		mapDriver.withOutput(new Text("key1"), new Text(""));
		mapDriver.withOutput(new Text("key2"), new Text(""));
		mapDriver.withOutput(new Text("key2"), new Text(""));
		mapDriver.withOutput(new Text("key2"), new Text(""));
		mapDriver.withOutput(new Text("key3"), new Text(""));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<Text> v1 = new ArrayList<Text>(Arrays.asList(new Text(""),new Text("")));
		List<Text> v2 = new ArrayList<Text>(Arrays.asList(new Text(""),new Text(""),new Text("")));
		List<Text> v3 = new ArrayList<Text>(Arrays.asList(new Text("")));
		reduceDriver.withInput(new Text("key1"), v1);
		reduceDriver.withInput(new Text("key2"), v2);
		reduceDriver.withInput(new Text("key3"), v3);
		reduceDriver.withOutput(new Text("key1"), new Text(""));
		reduceDriver.withOutput(new Text("key2"), new Text(""));
		reduceDriver.withOutput(new Text("key3"), new Text(""));
		reduceDriver.runTest();
	}

	@Test
	public void testMapperReducer() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("key1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("key1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("key2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("key2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("key2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("key3"));
		mapReduceDriver.withOutput(new Text("key1"), new Text(""));
		mapReduceDriver.withOutput(new Text("key2"), new Text(""));
		mapReduceDriver.withOutput(new Text("key3"), new Text(""));
		mapReduceDriver.runTest();
	}
}
