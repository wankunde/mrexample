package com.wankun.mr.hello;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

import static com.wankun.util.Utiltools.getStackTraceString;

/**
 * 数据去重复
 * 原理：将整条数据记录作为同一个key，这样的数据只输出一次
 * hadoop jar mrexample-1.0.0.jar com.wankun.mr.hello.Dedup
 *
 * @author wankun
 */
public class Dedup {
  private static final Log LOG = LogFactory.getLog(Dedup.class);

  // map将输入中的value复制到输出数据的key上，并直接输出
  public static class Map extends Mapper<Object, Text, Text, Text> {
    private static Text line = new Text();// 每行数据

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      LOG.info("mapper setup. Stack : " + getStackTraceString(new Throwable()));
    }

    // 实现map函数
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      line = value;
      context.write(line, new Text(""));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      LOG.info("mapper cleanup. Stack : " + getStackTraceString(new Throwable()));
    }
  }

  // reduce将输入中的key复制到输出数据的key上，并直接输出
  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    // 实现reduce函数
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      context.write(key, new Text(""));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] ioArgs = new String[]{"/tmp/wankun/tmp", "/tmp/wankun/tmp2"};
    String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.exit(2);
    }
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(new Path(ioArgs[1])))
      hdfs.delete(new Path(ioArgs[1]), true);

    Job job = Job.getInstance(conf, "Data Deduplication");
    job.setJarByClass(Dedup.class);
    // 设置Map、Combine和Reduce处理类
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    // 设置输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    // 设置输入和输出目录
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}