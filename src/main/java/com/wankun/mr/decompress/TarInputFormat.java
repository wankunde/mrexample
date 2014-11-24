package com.wankun.mr.decompress;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.common.base.Stopwatch;

public class TarInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new NullRecorderReader();
	}

	class NullRecorderReader extends RecordReader<LongWritable, Text> {
		private final Log LOG = LogFactory.getLog(NullRecorderReader.class);

		private Configuration conf = null;
		private TaskAttemptContext context = null;
		private FileSystem hdfs = null;

		private CompressionCodecFactory factory = null;
		private CompressionCodec codec = null;
		private boolean ignore = true;

		// 写文件变量
		private static final int BUFFER = 4096;
		private Path enPath = null;
		private OutputStream out = null;

		private Path input = null;
		private String destPath = null;
		private long srcsize = 0;
		// private int entrynum = 0;
		private int readentrynum = 0;
		private TarArchiveInputStream tais = null;
		private Text value = null;

		@Override
		public void close() throws IOException {
			if (tais != null)
				tais.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return new LongWritable(0);
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if (ignore)
				return 1.0f;
			else {
				LOG.info(" input:" + input.toString() + "  srcsize : "
						+ srcsize + " bytesRead :" + tais.getBytesRead()
						+ "  readentrynum:" + readentrynum);
				// + " entrynum:" + entrynum
				// return readentrynum * 1.0f / entrynum;
				return 0.5f;
			}
		}

		@Override
		public void initialize(InputSplit split,
				final TaskAttemptContext context) throws IOException,
				InterruptedException {
			this.conf = context.getConfiguration();
			this.context = context;
			this.hdfs = FileSystem.get(conf);
			this.input = ((FileSplit) split).getPath();

			if (input.getName().endsWith(".tar.gz")
					|| input.getName().endsWith(".tar.bz2"))
				this.ignore = false;
			else
				this.ignore = true;
			if (!ignore) {
				// 获取所拥有的所有压缩器——工厂
				this.factory = new CompressionCodecFactory(conf);
				// 1. 创建tar.gz 文件输出流 根据后缀得到相应的压缩器
				this.codec = factory.getCodec(input);

				this.destPath = CompressionCodecFactory.removeSuffix(
						input.toString(), codec.getDefaultExtension());
				if (this.destPath.endsWith("tar"))
					this.destPath = this.destPath.substring(0,
							this.destPath.length() - 4);

				hdfs.delete(new Path(destPath), true);

				// 从压缩输入流中读取内容放入文件输出流
				CompressionInputStream cin = codec.createInputStream(hdfs
						.open(input));
				tais = new TarArchiveInputStream(cin);

				this.srcsize = ((FileSplit) split).getLength();
			}
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (ignore)
				return false;

			Stopwatch watch = null;
			value = null;
			try {
				// 上一个文件不存在时，读新文件
				TarArchiveEntry entry = null;
				if (enPath == null) {
					entry = tais.getNextTarEntry();
					if (entry == null)
						return false;
					else {
						enPath = new Path(destPath, entry.getName());
						if (entry.isDirectory()) {
							hdfs.mkdirs(enPath);
							enPath = null;
							return true;
						} else {
							if (hdfs.exists(enPath))
								hdfs.delete(enPath, true);
							out = hdfs.create(enPath);
						}
					}
				}

				watch = new Stopwatch();
				watch.start();
				int count = -1;
				byte data[] = new byte[BUFFER];
				try {
					while ((count = tais.read(data, 0, BUFFER)) != -1
							&& watch.elapsedMillis() < 60000) {
						out.write(data, 0, count);
					}
				} catch (EOFException eof) {
					context.getCounter("decompress", "decompress error files")
					.increment(1l);
					value = new Text("文件读取失败：" + input.toString());
					LOG.error("文件读取失败：" + input.toString(), eof);
					count = -1;
					ignore=true;
				}

				if (count == -1) {
					readentrynum++;
					enPath = null;
					if (out != null) {
						out.flush();
						out.close();
						out = null;
					}
				}

				watch.stop();
				return true;
			} catch (Exception e) {
				StringBuilder errmsg = new StringBuilder();
				errmsg.append("解压文件错误 : ");
				if (input != null)
					errmsg.append(" input:" + input.toString());
				if (enPath != null)
					errmsg.append(" output:" + enPath.toString());
				LOG.error(errmsg.toString(), e);
				
				if (out != null) {
					out.flush();
					out.close();
					out = null;
				}
				if (enPath != null)
					hdfs.delete(enPath, true);
				throw e;
			} finally {
				if (watch != null)
					watch = null;
			}
		}
	}
}
