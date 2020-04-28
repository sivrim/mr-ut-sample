package org.sivrim;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@RunWith(JUnitPlatform.class)
public class AverageReducerTest {
	@Mock
	private Reducer<IntWritable, Occurrence, DoubleWritable, NullWritable>.Context mockContext;

	InOrder inOrder;

	AverageReducer reducer;

	@BeforeEach
	public void setUp() throws Exception {
		reducer = new AverageReducer();
		inOrder = inOrder(mockContext);
	}

	@Test
	public void testMapExactDivision() throws IOException, InterruptedException {
		Occurrence[] input = { new Occurrence(2, 3), new Occurrence(4, 6), new Occurrence(10, 1) };

		reducer.reduce(new IntWritable(1), Arrays.asList(input), mockContext);

		inOrder.verify(mockContext, times(1)).write(new DoubleWritable(4), NullWritable.get());

	}

	/**
	 * Some equality tests require custom matchers. In this case average will be
	 * something like 6.8943644...., which will fail equals.
	 */
	@Test
	public void testMapNeedsMatcher() throws IOException, InterruptedException {

		Occurrence[] input = { new Occurrence(7, 9), new Occurrence(4, 6), new Occurrence(11, 4) };

		reducer.reduce(new IntWritable(1), Arrays.asList(input), mockContext);
		DoubleMatcher averageMatcher = new DoubleMatcher(new DoubleWritable(6.89), .01);

		inOrder.verify(mockContext, times(1)).write(argThat(averageMatcher), eq(NullWritable.get()));
	}

	/**
	 * Direct exact match for Double fails.
	 */
	class DoubleMatcher implements ArgumentMatcher<DoubleWritable> {

		private DoubleWritable left;
		private double threshold = .01;

		public DoubleMatcher(DoubleWritable left, double threshold) {
			this.left = left;
			this.threshold = threshold;
		}

		@Override
		public boolean matches(DoubleWritable right) {
			boolean equals = Math.abs(left.get() - right.get()) < threshold;
			return equals;
		}
	}

	@AfterEach
	public void tearDown() throws Exception {
		verifyNoMoreInteractions(mockContext);
	}
}