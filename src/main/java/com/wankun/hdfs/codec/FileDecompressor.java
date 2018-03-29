package com.wankun.hdfs.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by WANKUN603 on 2017-03-16.
 */
public class FileDecompressor {
    public static void main(String[] args) throws Exception {

        String uri = "/user/hive/warehouse/base.db/uds_b_a_cust_wealth_labels/dt=20170315/000000_0";
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);

        //构建输入路径
        Path inputPath = new Path(uri);
        //创建CompressionCodecFactory对象
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        //获取文件的压缩格式
        CompressionCodec codec = factory.getCodec(inputPath);

        //如果压缩格式不存在就退出
        if (codec == null) {
            System.out.println("No codec found for " + uri);
        }

        //定义输入输出流
        InputStream in = null;

        Map<String, Object> data = new HashMap<>();
        Object o = new Object();

        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long maxHeap = memoryMXBean.getHeapMemoryUsage().getMax();

        try {

            Reader reader = OrcFile.createReader(fileSystem, inputPath);
            OutputStreamWriter out = new OutputStreamWriter(System.out, "UTF-8");
            RecordReader rows = reader.rows(null);
            Object row = null;
            List<OrcProto.Type> types = reader.getTypes();
            /*
            while (rows.hasNext()) {
                row = rows.next(row);
                JSONWriter writer = new JSONWriter(out);
                printObject(writer, row, types, 0);
                out.write("\n");
                out.flush();
            }

            OrcStruct struct = (OrcStruct)value;
            TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(SCHEMA);

            StructObjectInspector inspector = (StructObjectInspector)
                    OrcStruct.createObjectInspector(typeInfo);

            StringBuffer outputKey = new StringBuffer();


// OrcRecordReader reader = new OrcRecordReader(r,conf,0,747);
            OrcRecordReader reader = (OrcRecordReader) OrcInputFormat.createReaderFromFile(r, conf, 0, 747); //我的test.orc文件长度就是747，可以单独写个getFileSize方法获取
            if(reader!=null){
                System.out.println("========record counts : " + reader.numColumns);
                while(reader.nextKeyValue()){
                    OrcStruct data = reader.getCurrentValue();
                    System.out.println("fields: " + data.getNumFields());
                    for(int i = 0; i < data.getNumFields(); i++){
                        System.out.println("============" + data.getFieldValue(i));


            //创建输入输出流
            in = fileSystem.open(inputPath);
            if (codec != null) {
                in = codec.createInputStream(in);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                data.put(line, o);
                if (i < 100)
                    System.out.println(line);
                i++;
                if (i % 1000000 == 0) {
                    long usedMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
                    System.out.printf("row nums:" + i + "  memory used:" + usedMemory + "  percentage:" + (double) usedMemory / (double) maxHeap);
                }
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
