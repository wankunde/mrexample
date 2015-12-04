
### GenericOptionsParser

GenericOptionsParser
@see GenericOptionsParser
指定namenode的RPC地址进行查询，默认会读取配置文件，参数rpc-address支持指定namenode:8020和nameservices:8020两种形式。
 $ bin/hadoop dfs -fs darwin:8020 -ls /data
 list /data directory in dfs with namenode darwin:8020
 
 $ bin/hadoop dfs -D fs.default.name=darwin:8020 -ls /data
 list /data directory in dfs with namenode darwin:8020
     
 $ bin/hadoop dfs -conf hadoop-site.xml -ls /data
 list /data directory in dfs with conf specified in hadoop-site.xml
     
 $ bin/hadoop job -D mapred.job.tracker=darwin:50020 -submit job.xml
 submit a job to job tracker darwin:50020
     
 $ bin/hadoop job -jt darwin:50020 -submit job.xml
 submit a job to job tracker darwin:50020
     
 $ bin/hadoop job -jt local -submit job.xml
 submit a job to local runner
 
 $ bin/hadoop jar -libjars testlib.jar -archives test.tgz -files file.txt inputjar args
 job submission with libjars, files and archives
 

# comments

* eclipse中导入项目会报缺少 jdk.tools包，修改eclise.ini如下即可

	openFile
	--launcher.appendVmargs
	-vm
	C:\Program Files\Java\jdk1.8.0_45\bin\javaw.exe
	-vmargs
	-Dosgi.requiredJavaVersion=1.6