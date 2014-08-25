mrexample : Hadoop MapReduce程序编写
=========
## com.wankun.mr : WordCount mapreduce入门例子

http://www.cnblogs.com/xia520pi/archive/2012/06/04/2534533.html 贮备数据可以到这里取
1.1 hello.Dedup 数据去重复 
1.2 hello.Sort hadoop数据排序，可以自定义排序规则 

2. WordCount2 增加partitioner,combiner的模块的使用
3. partitioner.MyPartition2 测试partitioner的模块的使用
4. secondsort.SecondarySort 二次排序，注释比较详细
5. secondsort.MySecondSort2 一个比较好的二次排序的例子
6. counter.MyCounter 一个使用自定义计数器的例子
7. join.STjoin 表自身关联，通过关系 child - parent 找出 grandchild - grandparent 的关系

*  hdfs.HDFSTest hdfs文件的操作实战


## MR Unit 使用示例
	[MRUnit](https://cwiki.apache.org/confluence/display/MRUNIT/MRUnit+Tutorial) is testing framework for testing MapReduce programs written for running in Hadoop ecosystem. MRUnit makes testing Mapper and Reducer classes easier.

	MR单元测试：
	通过MapDriver对map的输入和输出进行单元测试
	通过ReduceDriver对reduce的输入和输出进行单元测试
	通过MapReduceDriver对map和reduce的输入和输出进行单元测试

### 示例程序说明		
	原始数据格式
``` 
CDRID;CDRType;Phone1;Phone2;SMS Status Code
655209;1;796764372490213;804422938115889;6
353415;0;356857119806206;287572231184798;4
835699;1;252280313968413;889717902341635;0
```
	com.wankun.mr.mrtest.SMSCDRMapper 如果CDRType字段为1，则输出<Status Code,1>键值对 
	com.wankun.mr.mrtest.SMSCDRReducer 对相同Status Code的值做sum统计
	com.wankun.mr.mrtest.SMSCDRMapperReducerTest Map和Reduce程序的单元测试
