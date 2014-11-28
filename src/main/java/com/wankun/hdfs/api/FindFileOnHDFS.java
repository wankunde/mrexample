package com.wankun.hdfs.api;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class FindFileOnHDFS {

	private static Configuration conf = new Configuration();
	private static FileSystem hdfs = null;
	public static void main(String[] args) throws Exception{
		hdfs = FileSystem.get(conf);
		getHDFSNodes();
		getFileLocal();
	}
     
	//查找某个文件在HDFS集群的位置
	public static void getFileLocal() throws Exception {
		Path fpath = new Path("/tmp/input");
        
		//获取文件系统里面的文件信息
		FileStatus fileStatus = hdfs.getFileStatus(fpath);
		//获取文件的块信息
		BlockLocation[] blkLocations = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

		int blockLen = blkLocations.length;
        
		//循环打印
		for(int i = 0 ; i < blockLen ; ++i ){
			String[] hosts = blkLocations[i].getHosts();
			System.out.println("block_"+i + "_location:" + hosts[i]);
		}
	}

	public static void getHDFSNodes() throws Exception{
		//获取所有的节点数
		DatanodeInfo[] dataNodeStats = ((DistributedFileSystem)hdfs).getDataNodeStats();
        
		//循环打印
		for( int i = 0 ; i < dataNodeStats.length ; ++i ){
			System.out.println("DataNode_" + i + "_Node:" + dataNodeStats[i].getHostName());
		}
	}
}
