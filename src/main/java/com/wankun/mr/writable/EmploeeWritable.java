package com.wankun.mr.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * http://blog.csdn.net/lastsweetop/article/details/9360075
 * @author wankun
 *
 */
public class EmploeeWritable implements WritableComparable<EmploeeWritable> {
	private Text name;
	private Text role;

	/**
	 * 必须有默认的构造器皿，这样Mapreduce方法才能创建对象，然后通过readFields方法从序列化的数据流中读出进行赋值
	 */
	public EmploeeWritable() {
		set(new Text(), new Text());
	}

	public EmploeeWritable(Text name, Text role) {
		set(name, role);
	}

	public void set(Text name, Text role) {
		this.name = name;
		this.role = role;
	}

	public Text getName() {
		return name;
	}

	public Text getRole() {
		return role;
	}

	/**
	 * 通过成员对象本身的write方法，序列化每一个成员对象到输出流中
	 * 
	 * @param dataOutput
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		name.write(dataOutput);
		role.write(dataOutput);
	}

	/**
	 * 同上调用成员对象本身的readFields方法，从输入流中反序列化每一个成员对象
	 * 
	 * @param dataInput
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		name.readFields(dataInput);
		role.readFields(dataInput);
	}

	/**
	 * implements WritableComparable必须要实现的方法,用于比较 排序
	 * 
	 * @param emploeeWritable
	 * @return
	 */
	@Override
	public int compareTo(EmploeeWritable emploeeWritable) {
		int cmp = name.compareTo(emploeeWritable.name);
		if (cmp != 0) {
			return cmp;
		}
		return role.compareTo(emploeeWritable.role);
	}

	/**
	 * MapReduce需要一个分割者（Partitioner）把map的输出作为输入分成一块块的喂给多个reduce）
	 * 默认的是HashPatitioner，他是通过对象的hashcode函数进行分割，所以hashCode的好坏决定
	 * 了分割是否均匀，他是一个很关键性的方法。
	 * 
	 * @return
	 */
	@Override
	public int hashCode() {
		return name.hashCode() * 163 + role.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof EmploeeWritable) {
			EmploeeWritable emploeeWritable = (EmploeeWritable) o;
			return name.equals(emploeeWritable.name) && role.equals(emploeeWritable.role);
		}
		return false;
	}

	/**
	 * 如果你想自定义TextOutputformat作为输出格式时的输出，你需要重写toString方法
	 * 
	 * @return
	 */
	@Override
	public String toString() {
		return name + "\t" + role;
	}
	
}
