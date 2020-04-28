package org.sivrim;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class AverageCombiner extends Reducer<IntWritable, IntWritable, IntWritable, Occurrence> {
	public static Logger log = Logger.getLogger(AverageCombiner.class);

	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (@SuppressWarnings("unused")
		IntWritable intWritable : values) {
			count++;
		}
		context.write(new IntWritable(1), new Occurrence(key, new IntWritable(count)));
	}
}