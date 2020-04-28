package org.sivrim;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class AverageReducer extends Reducer<IntWritable, Occurrence, DoubleWritable, NullWritable> {
	public static Logger log = Logger.getLogger(AverageReducer.class);

	@Override
	public void reduce(IntWritable key, Iterable<Occurrence> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		int sum = 0;
		for (@SuppressWarnings("unused")
		Occurrence occurrence : values) {
			count += occurrence.getFrequency().get();
			sum += occurrence.getFrequency().get() * occurrence.getNumber().get();
		}
		double average = (double) sum / (double) count;
		context.write(new DoubleWritable(average), NullWritable.get());
	}
}