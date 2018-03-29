package com.wankun.hdfs.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by WANKUN603 on 2017-08-28.
 */
public class SplitableCheck {
    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        final FileSystem fs = FileSystem.get(conf);
        Path file = new Path(args[0]);
        final CompressionCodec codec =
                new CompressionCodecFactory(fs.getConf()).getCodec(file);
        System.out.println("codec : " + codec);
        System.out.println("splitable : " + (codec instanceof SplittableCompressionCodec));
    }
}
