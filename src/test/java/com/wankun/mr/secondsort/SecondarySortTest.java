package com.wankun.mr.secondsort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class SecondarySortTest {
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	private SecondarySort.SortComparator comparator;
	private SecondarySort.GroupingComparator groupcomp;

	@Before
	public void setUp() {
		SecondarySort.Map mapper = new SecondarySort.Map();
		SecondarySort.Reduce reducer = new SecondarySort.Reduce();
		comparator = new SecondarySort.SortComparator();
		groupcomp = new SecondarySort.GroupingComparator();
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		
	}

	@Test
	public void testSecondSort() throws IOException {
		mapReduceDriver.setKeyOrderComparator(comparator);
		mapReduceDriver.withInput(new LongWritable(), new Text("1:3"));
		mapReduceDriver.withInput(new LongWritable(), new Text("3:2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2:3"));
		mapReduceDriver.withInput(new LongWritable(), new Text("1:2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2:1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("1:1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("3:1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2:2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("3:3"));
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("1:3"), new IntWritable());
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("1:2"), new IntWritable());
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("1:1"), new IntWritable());
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("2:3"), new IntWritable());
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("2:2"), new IntWritable());
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("2:1"), new IntWritable());
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("3:3"), new IntWritable());
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("3:2"), new IntWritable());
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("3:1"), new IntWritable());
		mapReduceDriver.runTest();
	}
	
	/**
	 * 根据第一列数据分组，同一组数据调用一次reduce，便于组内记录传递信息（必须排序），则value放在同一迭代器中，
	 * 排序使用默认字符串排序
	 * @throws IOException
	 */
	@Test
	public void testGroupCompare() throws IOException {
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
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("1:1"), new IntWritable());
		mapReduceDriver.withOutput(new Text("1:2"), new IntWritable());
		mapReduceDriver.withOutput(new Text("1:3"), new IntWritable());
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("2:1"), new IntWritable());
		mapReduceDriver.withOutput(new Text("2:2"), new IntWritable());
		mapReduceDriver.withOutput(new Text("2:3"), new IntWritable());
		mapReduceDriver.withOutput(new Text("-------------------------"), new IntWritable(1));
		mapReduceDriver.withOutput(new Text("3:1"), new IntWritable());
		mapReduceDriver.withOutput(new Text("3:2"), new IntWritable());
		mapReduceDriver.withOutput(new Text("3:3"), new IntWritable());
		mapReduceDriver.runTest();
	}
}
