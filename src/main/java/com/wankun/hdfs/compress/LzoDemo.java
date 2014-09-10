package com.wankun.hdfs.compress;

import java.io.BufferedInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * <pre>
 * Lzo 压缩文件
 * 
 *  1. 安装lzo和lzop工具包
 *  2. 引入hadoop-lzo类库
 *  	Apache（HDP版本测试 hadoop-lzo-0.6.0）
 *  		安装apache的lzo库，并在maven中引用
 *  		mvn install:install-file -Dfile=/usr/lib/hadoop/lib/hadoop-lzo-0.6.0.jar -DgroupId=hadoop-lzo -DartifactId=hadoop-lzo -Dversion=0.6.0 -Dpackaging=jar
 *  3. 添加native库（java.library.path）的地址
 *  	方法一：在eclipse的库JRE中有一个选项，更改为/usr/lib/hadoop/lib/native/Linux-amd64-64/即可
 *  	方法二：在vm arguments中添加-Djava.library.path=/usr/lib/hadoop/lib/native/Linux-amd64-64
 *  	方法三：export LD_LIBRARY_PAth=/usr/lib/hadoop/lib/native/Linux-amd64-64/
 *  
 *  4.使用lzo的本地库
 * 		在完成步骤三后，可以去加载so库。默认hadoop-lzo库中会自动加载 libgplcompression.so,加载代码 System.loadLibrary("gplcompression");
 * 		
 * 		如不设置java.library.path，也可以加载绝对路径  System.load("/usr/lib/hadoop/lib/native/Linux-amd64-64/libgplcompression.so");
 *  
 *  5. 使用lzo的编码解码器操作数据文件
 *  
 *  cloudera lzo 项目地址：https://github.com/cloudera/hadoop-lzo
 * </pre>
 * 
 * @author wankun
 * @date 2014年9月3日
 * @version 1.0
 */
public class LzoDemo {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// 准备编码解码器
		String codecClassname = "com.hadoop.compression.lzo.LzopCodec";
		Class<?> codecClass = Class.forName(codecClassname);
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

		// 创建文件输入输出流
		String inputpath = "/tmp/input";
		String outputpath = inputpath + ".lzo";
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(inputpath);
		Path outputPath = new Path(outputpath); // 注意，这个路径应该在hdfs中不存在，或者在用到的时候做逻辑处理
		BufferedInputStream is = new BufferedInputStream(fs.open(inputPath)); // 注意创建FSDataInputStream的方式，用fs对象的open方法
		FSDataOutputStream os = fs.create(outputPath); // 创建输出流

		// 关键点：创建压缩输出流
		CompressionOutputStream out = codec.createOutputStream(os); //
		IOUtils.copyBytes(is, out, 4096, true); // true表示完成之后关闭输入、输出流
	}
}