package com.wankun.hdfs.compress;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;


public class SequenceFileTest {

	/**
	 * 定义自己 的IO协议，实现如何输入输出 1.这里实现Writable接口，并实现write和readFields方法
	 * 
	 * @author wankun
	 * 
	 */
	public static class UserWritable implements Writable, Comparable {
		private long userId;
		private String userName;
		private int userAge;

		public long getUserId() {
			return userId;
		}

		public void setUserId(long userId) {
			this.userId = userId;
		}

		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public int getUserAge() {
			return userAge;
		}

		public void setUserAge(int userAge) {
			this.userAge = userAge;
		}

		public UserWritable(long userId, String userName, int userAge) {
			super();
			this.userId = userId;
			this.userName = userName;
			this.userAge = userAge;
		}

		public UserWritable() {
			super();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(this.userId);
			out.writeUTF(this.userName);
			out.writeInt(this.userAge);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.userId = in.readLong();
			this.userName = in.readUTF();
			this.userAge = in.readInt();
		}

		@Override
		public String toString() {
			return new ToStringBuilder(this).append("userId", this.userId).append("userName", this.userName)
					.append("userAge", this.userAge).toString();
		}

		/**
		 * 只对比userId
		 */
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof UserWritable) {
				UserWritable u1 = (UserWritable) obj;
				return u1.getUserId() == this.getUserId();
			}
			return false;
		}

		/**
		 * 只对比userId
		 */
		@Override
		public int compareTo(Object obj) {
			int result = -1;
			if (obj instanceof UserWritable) {
				UserWritable u1 = (UserWritable) obj;
				if (this.userId > u1.userId) {
					result = 1;
				} else if (this.userId == u1.userId) {
					result = 1;
				}
			}
			return result;
		}

		@Override
		public int hashCode() {
			return (int) this.userId & Integer.MAX_VALUE;
		}

	}

	/**
	 * 写入到sequence file
	 * 
	 * @param filePath
	 * @param conf
	 * @param datas
	 */
	public static void write2SequenceFile(String filePath, Configuration conf, Collection<UserWritable> datas) {
		FileSystem fs = null;
		SequenceFile.Writer writer = null;
		Path path = null;
		LongWritable idKey = new LongWritable(0);

		try {
			fs = FileSystem.get(conf);
			path = new Path(filePath);
			writer = SequenceFile.createWriter(fs, conf, path, LongWritable.class, UserWritable.class);

			for (UserWritable user : datas) {
				idKey.set(user.getUserId()); // userID为Key
				writer.append(idKey, user);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	/**
	 * 从sequence file文件中读取数据
	 * 
	 * @param sequeceFilePath
	 * @param conf
	 * @return
	 */
	public static List<UserWritable> readSequenceFile(String sequeceFilePath, Configuration conf) {
		List<UserWritable> result = null;
		FileSystem fs = null;
		SequenceFile.Reader reader = null;
		Path path = null;
		Writable key = null;
		UserWritable value = new UserWritable();

		try {
			fs = FileSystem.get(conf);
			result = new ArrayList<UserWritable>();
			path = new Path(sequeceFilePath);
			reader = new SequenceFile.Reader(fs, path, conf);
			key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf); // 获得Key，也就是之前写入的userId
			while (reader.next(key, value)) {
				result.add(value);
				value = new UserWritable();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
		return result;
	}

	private static Configuration getDefaultConf() {
		Configuration conf = new Configuration();
		// conf.set("mapred.job.tracker", "local");
		// conf.set("fs.default.name", "file:///");
		// conf.set("io.compression.codecs",
		// "com.Hadoop.compression.lzo.LzoCodec");
		return conf;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String filePath = "data/user.sequence"; // 文件路径
		Set<UserWritable> users = new HashSet<UserWritable>();
		UserWritable user = null;
		// 生成数据
		for (int i = 1; i <= 10; i++) {
			user = new UserWritable(i + (int) (Math.random() * 100000), "name-" + (i + 1),
					(int) (Math.random() * 50) + 10);
			users.add(user);
		}
		// 写入到sequence file
		write2SequenceFile(filePath, getDefaultConf(), users);
		// 从sequence file中读取
		List<UserWritable> readDatas = readSequenceFile(filePath, getDefaultConf());

		// 对比数据是否正确并输出
		for (UserWritable u : readDatas) {
			if (users.contains(u)) {
				System.out.println(u.toString());
			} else {
				System.err.println("Error data:" + u.toString());
			}
		}

	}

}