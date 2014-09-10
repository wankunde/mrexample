package com.wankun.hdfs.api;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSTest {

	// 在指定位置新建一个文件，并写入字符
	public static void WriteToHDFS(String file, String words) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		Path path = new Path(file);
		FSDataOutputStream out = fs.create(path); // 创建文件

		// 两个方法都用于文件写入，好像一般多使用后者
		out.writeBytes(words);
		out.write(words.getBytes("UTF-8"));

		out.close();
		// 如果是要从输入流中写入，或是从一个文件写到另一个文件（此时用输入流打开已有内容的文件）
		// 可以使用如下IOUtils.copyBytes方法。
		// FSDataInputStream in = fs.open(new Path(args[0]));
		// IOUtils.copyBytes(in, out, 4096, true) //4096为一次复制块大小，true表示复制完成后关闭流
	}

	public static void ReadFromHDFS(String file) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		Path path = new Path(file);
		FSDataInputStream in = fs.open(path);

		IOUtils.copyBytes(in, System.out, 4096, true);
		// 使用FSDataInoutStream的read方法会将文件内容读取到字节流中并返回
		/**
		 * FileStatus stat = fs.getFileStatus(path); // create the buffer byte[]
		 * buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
		 * is.readFully(0, buffer); is.close(); fs.close(); return buffer;
		 */
	}

	public static void DeleteHDFSFile(String file) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		Path path = new Path(file);
		// 查看fs的delete API可以看到三个方法。deleteonExit实在退出JVM时删除，下面的方法是在指定为目录是递归删除
		fs.delete(path, true);
		fs.close();
	}

	public static void UploadLocalFileHDFS(String src, String dst) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		Path pathDst = new Path(dst);
		Path pathSrc = new Path(src);

		fs.copyFromLocalFile(pathSrc, pathDst);
		fs.close();
	}

	public static void ListDirAll(String DirFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(DirFile), conf);
		Path path = new Path(DirFile);

		FileStatus[] status = fs.listStatus(path);
		// 方法1
		for (FileStatus f : status) {
			System.out.println(f.getPath().toString());
		}
		// 方法2
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p : listedPaths) {
			System.out.println(p.toString());
		}
	}

	public static void putMerge(String LocalSrcDir, String fsFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);// fs是HDFS文件系统
		FileSystem local = FileSystem.getLocal(conf); // 本地文件系统

		Path localDir = new Path(LocalSrcDir);
		Path HDFSFile = new Path(fsFile);

		FileStatus[] status = local.listStatus(localDir); // 得到输入目录
		FSDataOutputStream out = fs.create(HDFSFile); // 在HDFS上创建输出文件

		for (FileStatus st : status) {
			Path temp = st.getPath();
			FSDataInputStream in = local.open(temp);
			IOUtils.copyBytes(in, out, 4096, false); // 读取in流中的内容放入out
			in.close(); // 完成后，关闭当前文件输入流
		}
		out.close();
	}

	public static void getMerge(String srcDir, String localFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf); // fs是HDFS文件系统
		FileSystem local = FileSystem.getLocal(conf); // 本地文件系统

		Path localDir = new Path(srcDir);
		Path HDFSFile = new Path(localFile);

		FileStatus[] status = fs.listStatus(localDir); // 得到输入目录
		FSDataOutputStream out = local.create(HDFSFile); // 在HDFS上创建输出文件

		for (FileStatus st : status) {
			Path temp = st.getPath();
			FSDataInputStream in = fs.open(temp);
			IOUtils.copyBytes(in, out, 4096, false); // 读取in流中的内容放入out
			in.close(); // 完成后，关闭当前文件输入流
		}
		out.close();
	}

	public static void hdfsMerge(String srcDir, String fsFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf); // fs是HDFS文件系统

		Path localDir = new Path(srcDir);
		Path HDFSFile = new Path(fsFile);

		FileStatus[] status = fs.listStatus(localDir); // 得到输入目录
		FSDataOutputStream out = fs.create(HDFSFile); // 在HDFS上创建输出文件

		for (FileStatus st : status) {
			Path temp = st.getPath();
			FSDataInputStream in = fs.open(temp);
			IOUtils.copyBytes(in, out, 4096, false); // 读取in流中的内容放入out
			in.close(); // 完成后，关闭当前文件输入流
		}
		out.close();
	}

	public static void main(String[] args) throws IOException, URISyntaxException {
		// 下面做的是显示目录下所有文件
		ListDirAll("hdfs://mycluster/tmp");

		String fileWrite = "hdfs://mycluster/tmp/test/FileWrite";
		String words = "This words is to write into file!\n";
		WriteToHDFS(fileWrite, words);
		// 这里我们读取fileWrite的内容并显示在终端
		ReadFromHDFS(fileWrite);
		// 这里删除上面的fileWrite文件
		DeleteHDFSFile(fileWrite);
		// 假设本地有一个uploadFile，这里上传该文件到HDFS
		// String LocalFile =
		// "file:///home/kqiao/hadoop/MyHadoopCodes/uploadFile";
		// UploadLocalFileHDFS(LocalFile, fileWrite );

		// putMerge("/tmp/input4", "hdfs:///tmp/merged");
		getMerge("/tmp/input5", "/tmp/localmergerd");
		// hdfsMerge("/tmp/input5","/tmp/hdfsmergerd");
	}
}