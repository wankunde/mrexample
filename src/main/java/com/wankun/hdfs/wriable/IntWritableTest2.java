package com.wankun.hdfs.wriable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class IntWritableTest2 {
	public static void main(String[] args) throws IOException {
		DataOutputStream dataOut = null;
		DataInputStream dataIn = null;
		DataOutputStream dataOut2 = null;
		try {
			// 序列化
			IntWritable num1 = new IntWritable();
			num1.set(125);
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			dataOut = new DataOutputStream(byteOut);
			num1.write(dataOut);
			// 反序列化
			ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
			dataIn = new DataInputStream(byteIn);
			num1.readFields(dataIn);
			System.out.println(num1.get());
			
			// 比较 WritableComparable
			IntWritable num2 = new IntWritable();
			num2.set(126);
			System.out.println(num1.compareTo(num2));
			
			// 比较 RawComparator
			@SuppressWarnings("unchecked")
			RawComparator<IntWritable> intWritableComparator = WritableComparator.get(IntWritable.class);
			System.out.println(intWritableComparator.compare(num1, num2));
			ByteArrayOutputStream byteOut2 = new ByteArrayOutputStream();
			dataOut2 = new DataOutputStream(byteOut2);
			num2.write(dataOut2);
			byte[] b1 = byteOut.toByteArray();
			byte[] b2 = byteOut2.toByteArray();
			// 这个地方在比较两个byte数组的值
			// 实现原理：对于基础数据类型，还是要将数据读出来进行比较的，对于复杂的数据类型可以自定义比较顺序的字段进行key比较
			System.out.println(intWritableComparator.compare(b1, 0, b1.length, b2, 0, b2.length));
		} finally {
			IOUtils.closeQuietly(dataOut);
			IOUtils.closeQuietly(dataIn);
			IOUtils.closeQuietly(dataOut2);
		}
	}
}
