# Hadoop MapReduce编程

1. WordCount mapreduce入门例子

* [cache.CacheDemo](cache/CacheDemo.java) 通过API使用DistributedCache
* [cache.CacheDemo](cache/CacheDemo2.java) 通过，命令行使用DistributedCache
* [counter.MyCounter](counter.MyCounter.java) 在MR中自定义Counter
* [hello.Dedup](hello.Dedup.java) 实现数据去除重复
* [job.JobInfo](job.JobInfo.java) 在Job完成后获取Job信息
* [join.STjoin](join.STjoin.java) 实现表的自身关联
* [partitioner.MyPartitioner](partitioner.MyPartitioner.java) 自定义Partitioner
* [partitioner.TotalSortMR](partitioner.TotalSortMR.java) 使用了Partitioner中的全局排序和数据采用
* [secondsort.SecondarySort](secondsort.SecondarySort.java) 通过重写Key的比较器实现数据的二次排序
* [secondsort.SecondarySort2](secondsort.SecondarySort2.java) 通过组合Key对象实现数据的二次排序

2. MR 单元测试
	
MRUnit new API

 	Job job = new Job();
	mapDriver = MapDriver.newMapDriver(mapper).withConfiguration(job.getConfiguration());
	job.setInputFormatClass(AvroKeyInputFormat.class);
	job.setOutputFormatClass(AvroKeyOutputFormat.class);
	AvroJob.setInputKeySchema(job, MyAvro.SCHEMA$);
	AvroJob.setMapOutputKeySchema(job, MyAvro.SCHEMA$);
	AvroJob.setOutputKeySchema(job, MyAvro.SCHEMA$);