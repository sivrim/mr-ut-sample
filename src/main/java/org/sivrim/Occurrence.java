package org.sivrim;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Occurrence implements WritableComparable<Occurrence> {
	private IntWritable number;
	private IntWritable frequency;

	@Override
	public void readFields(DataInput in) throws IOException {
		number.readFields(in);
		frequency.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		number.write(out);
		frequency.write(out);
	}

	@Override
	public boolean equals(Object obj) {
		Occurrence that = (Occurrence) obj;
		return this.getFrequency().equals(that.getFrequency()) && this.getNumber().equals(that.getNumber());
	}

	public Occurrence() {
	}

	public Occurrence(IntWritable number, IntWritable frequency) {
		this.number = number;
		this.frequency = frequency;
	}

	public Occurrence(int number, int frequency) {
		this.number = new IntWritable(number);
		this.frequency = new IntWritable(frequency);
	}

	@Override
	public int compareTo(Occurrence that) {
		return getNumber().compareTo(that.getNumber());
	}

	public IntWritable getNumber() {
		return number;
	}

	public void setNumber(IntWritable number) {
		this.number = number;
	}

	public IntWritable getFrequency() {
		return frequency;
	}

	public void setFrequency(IntWritable frequency) {
		this.frequency = frequency;
	}

}
