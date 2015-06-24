package com.wankun.hdfs.wriable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.matchers.JUnitMatchers.*;

public class IntWritableTest {

	/**
	 * object -> byte
	 * 
	 * @param i
	 * @return
	 * @throws IOException
	 */
	public static byte[] serialize(IntWritable i) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(out);
		i.write(dout);
		dout.close();
		return out.toByteArray();
	}

	/**
	 * byte -> object
	 * 
	 * @param dest
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static byte[] deserialize(Writable dest, byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(in);
		dest.readFields(din);
		din.close();
		return bytes;
	}

	@Test
	public void testIntWritable() throws IOException {
		IntWritable i = new IntWritable(21);
		byte[] bytes = serialize(i);
		assertThat(bytes.length, is(4));
		assertThat(StringUtils.byteToHexString(bytes), is("00000015"));

		IntWritable j = new IntWritable();
		deserialize(j, bytes);
		assertEquals(i, j);
		assertThat(j.get(), is(21));
	}
}
