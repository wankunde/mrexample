package com.wankun.hdfs.parquet;

import static parquet.hadoop.ParquetFileWriter.MAGIC;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import com.google.common.base.Preconditions;

public class ParquetDemo {
	private static final Log LOG = LogFactory.getLog(ParquetDemo.class);

	public static Configuration conf = new Configuration();

	public static void main(String[] args) throws IOException {
//		testReadMeta();
		testRead();
	}

	public static void testRead() throws IOException {
		Path pfile = new Path("src/main/testdata/part-m-00000.parquet");
		ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, pfile);
		MessageType schema = readFooter.getFileMetaData().getSchema();
		System.out.println(schema);
		GroupWriteSupport.setSchema(schema, conf);
	}

	private static ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

	/**
	 * <pre>
	 * 0      4   175    362         366    370
	 * |      |    |      |            |      | 
	 * +------+----+------+------------+------+ 
	 * |MAGIC |data|footer|footer index|MAGIC | 
	 * +------+----+------+------------+------v
	 * </pre>
	 * 
	 * metadata 第一部分为schema，可以看到两个字段 age和sex，以及字段的属性定义信息
	 * 
	 * @throws IOException
	 */
	public static void testReadMeta() throws IOException {
		String filepath = "src/main/testdata/194189431b0fed7a-363bc47791c94697_633766864_data.0";
		RandomAccessFile f = new RandomAccessFile(filepath, "rw");

		long l = f.length();
		int FOOTER_LENGTH_SIZE = 4;
		Preconditions.checkArgument(l >= MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length, filepath
				+ " is not a Parquet file (too small)");
		long footerLengthIndex = l - FOOTER_LENGTH_SIZE - MAGIC.length;
		LOG.info("reading footer index at " + footerLengthIndex);

		f.seek(footerLengthIndex);

		// channel.seek(footerLengthIndex);
		int footerLength = readIntLittleEndian(f);
		byte[] magic = new byte[MAGIC.length];
		f.readFully(magic);
		if (!Arrays.equals(MAGIC, magic)) {
			throw new RuntimeException(filepath + " is not a Parquet file. expected magic number at tail "
					+ Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
		}
		long footerIndex = footerLengthIndex - footerLength;
		LOG.info("read footer length: " + footerLength + ", footer index: " + footerIndex);
		if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
			throw new RuntimeException("corrupted file: the footer index is not within the file");
		}
		f.seek(footerIndex);

		FileInputStream fin = new FileInputStream(filepath);
		fin.skip(footerIndex);
		ParquetMetadata metadata = parquetMetadataConverter.readParquetMetadata(fin);

		LOG.info("struct \t:  MAGIC + data + footer + footerIndex + MAGIC");

		LOG.info("footer \t: " + footerIndex);
		LOG.info("footerIndex\t: " + footerLengthIndex);
		LOG.info("filelength \t: " + l);

		System.out.println(ParquetMetadata.toJSON(metadata));
		System.out.println(ParquetMetadata.toPrettyJSON(metadata));
	}

	public static int readIntLittleEndian(RandomAccessFile f) throws IOException {
		int ch1 = f.read();
		int ch2 = f.read();
		int ch3 = f.read();
		int ch4 = f.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0) {
			throw new EOFException();
		}
		return ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0));
	}

}
