package com.demo.proj.GroupByDemo.controller;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.demo.proj.GroupByDemo.domain.Stbprp;

/**
 * 实现类似于sql的分组统计功能
 * 
 * addvcd,stcd,stnm,lgtd,lttd,stlc
 * 
 * 500112	607H5610	双                        	106.671367	30.0829	       北区竹镇华蓥3组                               
 * 500231	605H0938	迎                         	107.346685	30.000159	江县周镇前峰村4社                             
 * 500224	607H3576	华                         	105.9133	29.850494	梁县平镇四方村6组                             
 * 500106	607H1700	梁桥                        	106.354242	29.641569	坝区土镇五一村四塘社                        
 * 500243	608H4388	大河                        	108.253133	29.518753	水县龙镇东方村2组                             
 * 500240	605H1266	木乡                        	108.481139	30.243861	柱县枫乡莲花村街上组    
 * ...
 * 统计每个行政区下有多少个测站
 *
 */
public class GroupByDemo {

	public static class WcMapper extends Mapper<Object, Text, Text, Stbprp> {

		private Text mapKey = new Text();
		Stbprp st = new Stbprp();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Stbprp>.Context context)
				throws IOException, InterruptedException {
			String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
			String[] splits = line.split("\\s+");

			st.setAddvcd(Integer.valueOf(splits[0]));
			st.setStcd(splits[1]);
			st.setStnm(splits[2]);
			st.setLgtd(Double.valueOf(splits[3]));
			st.setLttd(Double.valueOf(splits[4]));
			st.setStlc(splits[5]);

			mapKey.set(splits[0]);

			context.write(mapKey, st);
		}
	}

	public static class WcReducer extends Reducer<Text, Stbprp, Text, IntWritable> {

		IntWritable out = new IntWritable();

		@Override
		protected void reduce(Text arg0, Iterable<Stbprp> arg1, Reducer<Text, Stbprp, Text, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Stbprp st : arg1) {
				sum++;
			}
			out.set(sum);
			arg2.write(arg0, out);
		}
	}

	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();

			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length != 2) {
				System.err.println(otherArgs.length);
				System.err.println("Usage: wordcount <in> <out>");
				System.exit(2);
			}
			
			FileSystem fs = FileSystem.newInstance(new URI("hdfs://192.168.128.140:9000"), conf);
			Path path = new Path("/user/hadoop/output");
			if (fs.exists(path)) {
				boolean delete = fs.delete(path, true);

				if (!delete) {
					System.out.println("删除output目录失败");
					System.exit(3);
				}
			}
			
			Job job = Job.getInstance(conf, "word count");
			job.setJarByClass(GroupByDemo.class);
			job.setMapperClass(WcMapper.class);
			job.setReducerClass(WcReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Stbprp.class);

			// job.setNumReduceTasks(1);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

	}
}
