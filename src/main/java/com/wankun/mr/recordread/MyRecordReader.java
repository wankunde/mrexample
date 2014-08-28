package com.wankun.mr.recordread;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class MyRecordReader implements RecordReader<LongWritable, Text> {
	JobConf jobConf = null;
	FileSplit fileSplit = null;
	long currentPosition = 0;
	FileSystem fileSystem = null;
	FSDataInputStream inputStream = null;

	public MyRecordReader(JobConf job, FileSplit split) throws IOException {
		this.jobConf = job;
		this.fileSplit = split;

		fileSystem = fileSplit.getPath().getFileSystem(jobConf);
		inputStream = fileSystem.open(fileSplit.getPath());

	}

	String result = null;
	boolean processed = false;

	public boolean next(LongWritable key, Text value) throws IOException {

		processData(key, value);

		value.set(result);

		if (finished)
			return false;

		return true; // To change body of implemented methods use File |
						// Settings | File Templates.
	}

	boolean finished = false;

	private void processData(LongWritable key, Text value) throws IOException {

		// currentPosition = inputStream.getPos();
		System.out.println(fileSplit.getPath().toString());
		result = "";
		int len = 7;
		byte[] b = new byte[(int) fileSplit.getLength()];

		System.out.println("start" + fileSplit.getStart() + " current" + currentPosition + "  total:"
				+ fileSplit.getLength() + "  cur+len:" + (currentPosition + len));
		int currentPosition1 = (int) currentPosition;

		if (currentPosition + 3 >= fileSplit.getLength()) {
			finished = true;
			inputStream.readFully(b, currentPosition1, (int) (fileSplit.getLength() - currentPosition));
		} else {
			inputStream.read(currentPosition, b, currentPosition1, len);
			// inputStream.seek(currentPosition1);
			// IOUtils.readFully(inputStream, b, currentPosition1, len);

		}
		String tmp = new String(b);
		if (currentPosition < b.length) {
			result = new String(b, currentPosition1, len);
			key.set(currentPosition);
			value.set(result);
		}
		// IOUtils.readFully(inputStream, b, (int)currentPosition,
		// (int)(currentPosition+ len));

		currentPosition += len;

		processed = true;

		System.out.println("Next: " + result);

	}

	public LongWritable createKey() {
		return new LongWritable(currentPosition); // To change body of
													// implemented methods use
													// File | Settings | File
													// Templates.
	}

	public Text createValue() {

		/*
		 * if(result == null){ try { processData(); } catch (IOException e) {
		 * e.printStackTrace(); //To change body of catch statement use File |
		 * Settings | File Templates. } }
		 */

		return new Text("200050"); // To change body of implemented methods use
									// File | Settings | File Templates.
	}

	public long getPos() throws IOException {
		return currentPosition; // To change body of implemented methods use
								// File | Settings | File Templates.
	}

	public void close() throws IOException {
		inputStream.close();
	}

	public float getProgress() throws IOException {
		return processed ? 1.0F : 0.0F; // To change body of implemented methods
										// use File | Settings | File Templates.
	}
}