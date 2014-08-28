package com.wankun.mr.tool;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;

public class MRLoadMapper extends Mapper<LongWritable, Text, LongWritable, SearchLogProtobufWritable> {
	private final static Logger logger = Logger.getLogger(MRLoadMapper.class);
	private long id = 0;
	private final SimpleDateFormat sdf = new SimpleDateFormat("-dd/MMM/yyyy:HH:mm:ss Z");
	private final LongWritable outputkey = new LongWritable();
	private final SearchLogProtobufWritable outputvalue = new SearchLogProtobufWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		int taskid = context.getTaskAttemptID().getTaskID().getId();
		this.id = taskid << 40;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		final SearchLog.Builder builder;
		SearchLog msg = null;
		try {
			String[] cols = line.split("\\t");
			if (cols.length != 9) {
				context.getCounter("bad", "colsMissMatch").increment(1);
				logger.info("处理文件的第" + key.get() + "行内容时出错，内容为【" + line + "】,列数不符合");
				return;
			}
			String remote_addr = cols[0];
			String time_local = cols[1]; // todo: set time
			String remote_user = cols[2];
			String url = cols[3];
			String status = cols[4];
			String body_bytes_sent = cols[5];
			String http_user_agent = cols[6];
			String dummy2 = cols[7];
			String dummy3 = cols[8];

			builder = SearchLog.newBuilder().setRemoteAddr(remote_addr).setStatus(Integer.valueOf(status))
					.setId(++this.id).setTime(this.sdf.parse(time_local).getTime())
					.setBodyBytesSent(Integer.valueOf(body_bytes_sent)).setUa(http_user_agent);
			com.sunchangming.searchlog.LoadPlainLog.parseURL(url, builder);
			msg = builder.build();
		} catch (LocationCannotNull ex) {
			context.getCounter("bad", "lo为空").increment(1);
			context.getCounter("bad", "parseError").increment(1);
			logger.info("处理文件内容时出错，lo为空，内容为【" + line + "】");
		} catch (UriSchemeIsLocal ex) {
			context.getCounter("bad", "URIscheme是file").increment(1);
			context.getCounter("bad", "parseError").increment(1);
		} catch (UriSchemeError ex) {
			context.getCounter("bad", "UriSchemeError").increment(1);
			context.getCounter("bad", "parseError").increment(1);
		} catch (Exception ex) {
			context.getCounter("bad", "parseError").increment(1);
			logger.info("处理文件内容时出错，内容为【" + line + "】,出错信息是【" + ex.toString() + "】");
		}
		if (msg != null) {
			this.outputkey.set(this.id);
			this.outputvalue.set(msg);
			context.write(this.outputkey, this.outputvalue);
		}
	}

}