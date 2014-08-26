package com.wankun.mr.join;

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
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * <pre>
 * 原始数据：
 * factoryname addressID
 * Beijing Red Star 1
 * Guangzhou Honda 2
 * Beijing Rising 1
 * Guangzhou Development Bank 2
 * Back of Beijing 1
 * 
 * addressID addressname
 * 1 Beijing
 * 2 Guangzhou
 * 
 * 结果数据：
 * factoryname addressname
 * Beijing Red Star 	Beijing
 * Guangzhou Honda 	Guangzhou
 * Beijing Rising 	Beijing
 * Guangzhou Development Bank 	Guangzhou
 * Back of Beijing 	Beijing
 * 
 * </pre>
 * 
 * @author wankun
 * @date 2014年8月26日
 * @version 1.0
 */
public class STjoinTest {
	MapDriver<Object, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		STjoin.Map mapper = new STjoin.Map();
		STjoin.Reduce reducer = new STjoin.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("Beijing Red Star	1"));
		mapDriver.withInput(new LongWritable(), new Text("Guangzhou Honda	2"));
		mapDriver.withInput(new LongWritable(), new Text("Beijing Rising	1"));
		mapDriver.withInput(new LongWritable(), new Text("Guangzhou Development Bank	2"));
		mapDriver.withInput(new LongWritable(), new Text("Back of Beijing	1"));

		mapDriver.withInput(new LongWritable(), new Text("1	Beijing"));
		mapDriver.withInput(new LongWritable(), new Text("2	Guangzhou"));

		mapDriver.withOutput(new Text("1"), new Text("1+Beijing Red Star+1"));
		mapDriver.withOutput(new Text("Beijing Red Star"), new Text("2+Beijing Red Star+1"));
		mapDriver.withOutput(new Text("2"), new Text("1+Guangzhou Honda+2"));
		mapDriver.withOutput(new Text("Guangzhou Honda"), new Text("2+Guangzhou Honda+2"));
		mapDriver.withOutput(new Text("1"), new Text("1+Beijing Rising+1"));
		mapDriver.withOutput(new Text("Beijing Rising"), new Text("2+Beijing Rising+1"));
		mapDriver.withOutput(new Text("2"), new Text("1+Guangzhou Development Bank+2"));
		mapDriver.withOutput(new Text("Guangzhou Development Bank"), new Text("2+Guangzhou Development Bank+2"));
		mapDriver.withOutput(new Text("1"), new Text("1+Back of Beijing+1"));
		mapDriver.withOutput(new Text("Back of Beijing"), new Text("2+Back of Beijing+1"));

		mapDriver.withOutput(new Text("Beijing"), new Text("1+1+Beijing"));
		mapDriver.withOutput(new Text("1"), new Text("2+1+Beijing"));
		mapDriver.withOutput(new Text("Guangzhou"), new Text("1+2+Guangzhou"));
		mapDriver.withOutput(new Text("2"), new Text("2+2+Guangzhou"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<Text> values1 = new ArrayList<Text>(Arrays.asList(new Text("1+Beijing Red Star+1"), new Text(
				"1+Beijing Rising+1"), new Text("1+Back of Beijing+1"), new Text("2+1+Beijing")));
		List<Text> values2 = new ArrayList<Text>(Arrays.asList(new Text("1+Guangzhou Honda+2"), new Text(
				"1+Guangzhou Development Bank+2"), new Text("2+2+Guangzhou")));
		reduceDriver.withInput(new Text("1"), values1);
		reduceDriver.withInput(new Text("2"), values2);
		reduceDriver.withOutput(new Text("grandchild"), new Text("grandparent"));
		reduceDriver.withOutput(new Text("Beijing Red Star"), new Text("Beijing"));
		reduceDriver.withOutput(new Text("Beijing Rising"), new Text("Beijing"));
		reduceDriver.withOutput(new Text("Back of Beijing"), new Text("Beijing"));
		reduceDriver.withOutput(new Text("Guangzhou Honda"), new Text("Guangzhou"));
		reduceDriver.withOutput(new Text("Guangzhou Development Bank"), new Text("Guangzhou"));
		reduceDriver.runTest();
	}

	@Test
	public void testMapperReducer() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("Beijing Red Star	1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("Guangzhou Honda	2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("Beijing Rising	1"));
		mapReduceDriver.withInput(new LongWritable(), new Text("Guangzhou Development Bank	2"));
		mapReduceDriver.withInput(new LongWritable(), new Text("Back of Beijing	1"));

		mapReduceDriver.withInput(new LongWritable(), new Text("1	Beijing"));
		mapReduceDriver.withInput(new LongWritable(), new Text("2	Guangzhou"));

		// 单独测试时需要进行反注释
		// mapReduceDriver.withOutput(new Text("grandchild"), new Text("grandparent"));
		mapReduceDriver.withOutput(new Text("Beijing Red Star"), new Text("Beijing"));
		mapReduceDriver.withOutput(new Text("Beijing Rising"), new Text("Beijing"));
		mapReduceDriver.withOutput(new Text("Back of Beijing"), new Text("Beijing"));
		mapReduceDriver.withOutput(new Text("Guangzhou Honda"), new Text("Guangzhou"));
		mapReduceDriver.withOutput(new Text("Guangzhou Development Bank"), new Text("Guangzhou"));

		mapReduceDriver.runTest();
	}
}
