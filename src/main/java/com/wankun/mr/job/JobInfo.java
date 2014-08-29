package com.wankun.mr.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * hadoop jar com.wankun.mr.job.JobInfo job_1409121209604_0046	
 * @author root
 *
 */
public class JobInfo extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println(" JobInfo <job ID>");
			return -1;
		}
		String jobID = args[0];
		
		JobClient jobClient = new JobClient(new JobConf(getConf()));
		RunningJob job = jobClient.getJob(JobID.forName(jobID));
		if (job == null) {
			System.err.printf("No job with ID %s found.\n", jobID);
			return -1;
		}
		if (!job.isComplete()) {
			System.err.printf("Job %s is not complete.\n", jobID);
			return -1;
		}
		Counters counters = job.getCounters();
		
		long data_local_maps = counters.getCounter(JobCounter.DATA_LOCAL_MAPS);
		long mb_millis_maps=counters.getCounter(JobCounter.MB_MILLIS_MAPS);
		System.out.println("data_local_maps:"+data_local_maps);
		System.out.println("mb_millis_maps:"+mb_millis_maps);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JobInfo(), args);
		System.exit(exitCode);
	}
}
