package com.wankun.mr.counter;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class MyCounterTest {
	MapDriver<Object, Text, Text, IntWritable> mapDriver;

	@Before
	public void setUp() {
		MyCounter.TokenizerMapper mapper = new MyCounter.TokenizerMapper();
		mapDriver = new MapDriver<>(mapper);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(""));
		mapDriver.withInput(new LongWritable(), new Text(""));
		mapDriver.runTest();
		assertEquals("Expected 1 counter increment", 2, mapDriver.getCounters().findCounter("Error Rows", "Empty Row")
				.getValue());
	}

}
