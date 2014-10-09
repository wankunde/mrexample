package com.wankun.hdfs.snappy;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.IOUtils;
import org.xerial.snappy.Snappy;

public class SnappyDemo {
	public static void main(String[] args) throws UnsupportedEncodingException, IOException {
		testByte();
		testStream();
	}

	public static void testByte() throws UnsupportedEncodingException, IOException {
		String input = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
				+ "Snappy, a fast compresser/decompresser.";
		// byte[] ori = input.getBytes("UTF-8");
		byte[] compressed = Snappy.compress(input.getBytes("UTF-8"));
		byte[] uncompressed = Snappy.uncompress(compressed);

		String result = new String(uncompressed, "UTF-8");
		System.out.println(result);
	}

	public static void testStream() throws UnsupportedEncodingException, IOException {
		InputStream in = new FileInputStream(new File("README.md"));
		IOUtils.copyBytes(in, System.out, 1024,true);
	}
}
