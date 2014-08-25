package com.wankun.mr.mrtest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.wankun.mr.mrtest.SMSCDRMapper;
import com.wankun.mr.mrtest.SMSCDRReducer;
import com.wankun.mr.mrtest.SMSCDRMapper.CDRCounter;

/**
 * MRUnit new API
 * 
 * <pre>
 * Job job = new Job();
 * mapDriver = MapDriver.newMapDriver(mapper).withConfiguration(job.getConfiguration());
 * job.setInputFormatClass(AvroKeyInputFormat.class);
 * job.setOutputFormatClass(AvroKeyOutputFormat.class);
 * AvroJob.setInputKeySchema(job, MyAvro.SCHEMA$);
 * AvroJob.setMapOutputKeySchema(job, MyAvro.SCHEMA$);
 * AvroJob.setOutputKeySchema(job, MyAvro.SCHEMA$);
 * </pre>
 * 
 * @author wankun
 * @date 2014年8月22日
 * @version 1.0
 */
public class SMSCDRMapperReducerTest {

	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		SMSCDRMapper mapper = new SMSCDRMapper();
		SMSCDRReducer reducer = new SMSCDRReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		mapDriver.getConfiguration().set("myParameter1", "20");
		mapDriver.getConfiguration().set("myParameter2", "23");
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("655209;1;796764372490213;804422938115889;6"));
		mapDriver.withOutput(new Text("6"), new IntWritable(1));
		mapDriver.runTest();
		assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters().findCounter(CDRCounter.NonSMSCDR)
				.getValue());
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("6"), values);
		reduceDriver.withOutput(new Text("6"), new IntWritable(2));
		reduceDriver.runTest();
	}

	@Test
	public void testMapperReducer() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("655209;1;796764372490213;804422938115889;6"));
		mapReduceDriver.withInput(new LongWritable(), new Text("655209;1;796764372490213;804422938115889;7"));
		mapReduceDriver.withOutput(new Text("6"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("7"), new IntWritable(1));
		mapReduceDriver.runTest();
	}
}