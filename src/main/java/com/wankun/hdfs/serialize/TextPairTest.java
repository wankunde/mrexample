package com.wankun.hdfs.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.WritableComparator;

public class TextPairTest {
	public static void main(String[] args) throws IOException {
		DataOutputStream dataOut = null;
		DataInputStream dataIn = null;
		DataOutputStream dataOut2 = null;
		try {
			// 序列化
			TextPair tp = new TextPair();
			tp.set("name", "test");
			System.out.println("序列化前-----------");
			System.out.println(tp);
			ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
			dataOut = new DataOutputStream(byteOut);
			tp.write(dataOut);
			
			// 反序列化
			ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
			dataIn = new DataInputStream(byteIn);
			TextPair tp2 = new TextPair();
			tp2.readFields(dataIn);
			System.out.println("序列化后-----------");
			System.out.println(tp2);
			// 比较
			System.out.println(tp.equals(tp2));
			System.out.println(tp.compareTo(tp2));
			// RawCompartor
			byte[] b = byteOut.toByteArray();
			ByteArrayOutputStream byteOut2 = new ByteArrayOutputStream();
			dataOut2 = new DataOutputStream(byteOut2);
			tp2.write(dataOut2);
			byte[] b2 = byteOut2.toByteArray();
			System.out.println(WritableComparator.get(TextPair.class).compare(b, 0, b.length, b2, 0, b2.length));
		} finally {
			IOUtils.closeQuietly(dataOut);
			IOUtils.closeQuietly(dataIn);
			IOUtils.closeQuietly(dataOut2);
		}
	}
}
