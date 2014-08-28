package com.wankun.mr.recordread;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyFileInputFormat extends FileInputFormat {
	@Override
	public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

		return new MyRecordReader(job, (FileSplit) split);
	}
}