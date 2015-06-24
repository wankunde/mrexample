package com.wankun.hdfs.wriable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TextPair implements WritableComparable<TextPair> {
	private Text first;
	private Text second;

	public TextPair() {
		first = new Text();
		second = new Text();
	}

	public TextPair(String first, String second) {
		set(new Text(first), new Text(second));
	}

	public TextPair(Text first, Text second) {
		set(first, second);
	}

	public void set(String first, String second) {
		this.first.set(first);
		this.second.set(second);
	}

	public void set(Text first, Text second) {
		this.first = first;
		this.second = second;
	}

	public Text getFirst() {
		return first;
	}

	public Text getSecond() {
		return second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int compareTo(TextPair tp) {
		int rs = this.first.compareTo(tp.getFirst());
		if (rs == 0) {
			rs = this.second.compareTo(tp.getSecond());
		}
		return rs;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TextPair) {
			TextPair tp = (TextPair) obj;
			return this.first.equals(tp.getFirst()) && this.second.equals(tp.getSecond());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}
	
	// 注册到工厂类中 -- 不能写在静态类中，会注册失败
	static {
		WritableComparator.define(TextPair.class, new Comparator());
	}

	// RawComparator
	public static class Comparator extends WritableComparator {
		private WritableComparator textComparator = WritableComparator.get(Text.class);

		protected Comparator() {
			super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				// Text序列化的二进制包含UTF-8的字节数以及UTF-8字节本身
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b1[s2]) + readVInt(b2, s2);
				int rs = textComparator.compare(b1, s1, firstL1, b2, s2, firstL2);
				if (rs != 0) {
					return rs;
				}
				return textComparator.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return super.compare(b1, s1, l1, b2, s2, l2);
		}
	}
}
