package com.wankun.hdfs.wriable;

import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;

public class TextTest {

	public static void main(String[] args) throws UnsupportedEncodingException {
		teststring();
		testText();
		byteRead();
		testChangable();
	}

	public static void teststring() throws UnsupportedEncodingException {
		String s = "\u0041\u00DF\u6771\uD801\uDC00";
		assertEquals(s.length(), 5);
		assertEquals(s.getBytes("UTF-8").length, 10);
		assertEquals(s.indexOf("\u0041"), 0);
		assertEquals(s.indexOf("\u00DF"), 1);
		assertEquals(s.indexOf("\u6771"), 2);
		assertEquals(s.indexOf("\uD801\uDC00"), 3);
		assertEquals(s.charAt(0), '\u0041');
		assertEquals(s.charAt(1), '\u00DF');
		assertEquals(s.charAt(2), '\u6771');
		assertEquals(s.charAt(3), '\uD801');
		assertEquals(s.charAt(4), '\uDC00');
		assertEquals(s.codePointAt(0), 0x0041);
		assertEquals(s.codePointAt(1), 0x00DF);
		assertEquals(s.codePointAt(2), 0x6771);
		assertEquals(s.codePointAt(3), 0x10400);
	}

	public static void testText() {
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		assertEquals(t.getLength(), 10);
		// 字节数
		assertEquals(t.find("\u0041"), 0);
		assertEquals(t.find("\u00DF"), 1);
		assertEquals(t.find("\u6771"), 3);
		// 3：字节偏移量
		assertEquals(t.find("\uD801\uDC00"), 6);
		assertEquals(t.charAt(0), 0x0041);
		assertEquals(t.charAt(1), 0x00DF);
		assertEquals(t.charAt(3), 0x6771);
		// 3：字节偏移量
		assertEquals(t.charAt(6), 0x10400);
	}

	public static void byteRead() {
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		System.out.println(t.toString());
		ByteBuffer buf = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
		int cp;
		while (buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf)) != -1) {
			System.out.println(Integer.toHexString(cp));
		}
	}

	public static void testChangable() {
		Text text = new Text();
		text.set("hello,hadoop");
		text.set("test");
		// 之前书上说改变值后getBytes和getLength结果可能不一致，但是实际还是一致的
		assertEquals(text.getBytes().length, 4);
		assertEquals(text.getLength(), 4);
	}
}
