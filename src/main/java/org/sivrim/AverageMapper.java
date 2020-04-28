package org.sivrim;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class AverageMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	public static Logger log = Logger.getLogger(AverageMapper.class);

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int number = Integer.parseInt(value.toString());
		context.write(new IntWritable(number), new IntWritable(1));
	}
}