## MR Unit 使用示例

[MRUnit](https://cwiki.apache.org/confluence/display/MRUNIT/MRUnit+Tutorial) is testing framework for testing MapReduce programs written for running in Hadoop ecosystem. MRUnit makes testing Mapper and Reducer classes easier.

MR单元测试：
* 通过MapDriver对map的输入和输出进行单元测试
* 通过ReduceDriver对reduce的输入和输出进行单元测试
* 通过MapReduceDriver对map和reduce的输入和输出进行单元测试

	
### MRUnit new API
	
	Job job = new Job();
	mapDriver = MapDriver.newMapDriver(mapper).withConfiguration(job.getConfiguration());
	job.setInputFormatClass(AvroKeyInputFormat.class);
	job.setOutputFormatClass(AvroKeyOutputFormat.class);
	AvroJob.setInputKeySchema(job, MyAvro.SCHEMA$);
	AvroJob.setMapOutputKeySchema(job, MyAvro.SCHEMA$);
	AvroJob.setOutputKeySchema(job, MyAvro.SCHEMA$);