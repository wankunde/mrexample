# Hadoop MapReduce编程

1. WordCount mapreduce入门例子

* [cache.CacheDemo](cache/CacheDemo.java) 通过API使用DistributedCache
* [cache.CacheDemo2](cache/CacheDemo2.java) 通过，命令行使用DistributedCache
* [counter.MyCounter](counter/MyCounter.java) 在MR中自定义Counter
* [hello.Dedup](hello/Dedup.java) 实现数据去除重复
* [job.JobInfo](job/JobInfo.java) 在Job完成后获取Job信息
* [join.STjoin](join/STjoin.java) 实现表的自身关联
* [partitioner.MyPartitioner](partitioner/MyPartitioner.java) 自定义Partitioner
* [partitioner.TotalSortMR](partitioner/TotalSortMR.java) 使用了Partitioner中的全局排序和数据采用
* [secondsort.SecondarySort](secondsort/SecondarySort.java) 通过重写Key的比较器实现数据的二次排序
* [secondsort.SecondarySort2](secondsort/SecondarySort2.java) 通过组合Key对象实现数据的二次排序

* [decompress](secondsort) 重写InputFormat实现对tar.gz和tar.bz2文件的解压

2. MR 单元测试
	
MRUnit new API

 	Job job = new Job();
	mapDriver = MapDriver.newMapDriver(mapper).withConfiguration(job.getConfiguration());
	job.setInputFormatClass(AvroKeyInputFormat.class);
	job.setOutputFormatClass(AvroKeyOutputFormat.class);
	AvroJob.setInputKeySchema(job, MyAvro.SCHEMA$);
	AvroJob.setMapOutputKeySchema(job, MyAvro.SCHEMA$);
	AvroJob.setOutputKeySchema(job, MyAvro.SCHEMA$);
	
## file format
### Sequence File

> 来源：http://blog.csdn.net/xhh198781/article/details/7693358
	
SequenceFile文件是Hadoop用来存储二进制形式的key-value对而设计的一种平面文件(Flat File)。SequenceFile文件并不保证其存储的key-value数据是按照key的某个顺序存储的，同时不支持append操作。

* Uncompressed SequenceFile
![Uncompressed SequenceFile](/docs/pics/sequence_file1.png)
* Record-Compressed SequenceFile
![Record-Compressed SequenceFile](/docs/pics/sequence_file2.png)
* Block-Compressed SequenceFile
![Block-Compressed SequenceFile](/docs/pics/sequence_file3.png)

### writable

自定义writable对象用于读写数据
示例完成了writable类的创建，comparable比较器的重载
