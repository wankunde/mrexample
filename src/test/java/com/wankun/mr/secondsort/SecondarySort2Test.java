package com.wankun.mr.secondsort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.wankun.mr.secondsort.SecondarySort2.IntPair;

public class SecondarySort2Test {
	MapReduceDriver<LongWritable, Text, IntPair, IntWritable, Text, IntWritable> mapReduceDriver;
	private SecondarySort2.GroupingComparator groupcomp;

	@Before
	public void setUp() {
		SecondarySort2.Map mapper = new SecondarySort2.Map();
		SecondarySort2.Reduce reducer = new SecondarySort2.Reduce();
		groupcomp = new SecondarySort2.GroupingComparator();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		
	}

	@Test
	public void testSecondSort2() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("1:3"));
		mapReduceDriver.withInput(new LongWritable(), new Text("3:2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2:3"));
		mapReduceDriver.withInput(new LongWritable(), new Text("1:2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2:1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("1:1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("3:1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2:2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("3:3"));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("1"), new IntWritable(3));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("1"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("1"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("2"), new IntWritable(3));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("2"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("2"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("3"), new IntWritable(3));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("3"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("3"), new IntWritable(1));
		mapReduceDriver.runTest();
	}
	
	/**
	 * 根据第一列数据分组，同一组数据调用一次reduce，便于组内记录传递信息（必须排序），则value放在同一迭代器中，
	 * 排序使用默认字符串排序
	 * @throws IOException
	 */
	@Test
	public void testGroupCompare2() throws IOException {
		mapReduceDriver.setKeyGroupingComparator(groupcomp);
		mapReduceDriver.withInput(new LongWritable(), new Text("1:3"));
		mapReduceDriver.withInput(new LongWritable(), new Text("3:2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2:3"));
		mapReduceDriver.withInput(new LongWritable(), new Text("1:2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2:1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("1:1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("3:1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2:2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("3:3"));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("1"), new IntWritable(3));
		mapReduceDriver.withOutput(new Text("1"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("1"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("2"), new IntWritable(3));
		mapReduceDriver.withOutput(new Text("2"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("2"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable());
		mapReduceDriver.withOutput(new Text("3"), new IntWritable(3));
		mapReduceDriver.withOutput(new Text("3"), new IntWritable(2));
		mapReduceDriver.withOutput(new Text("3"), new IntWritable(1));
		mapReduceDriver.runTest();
	}
}
