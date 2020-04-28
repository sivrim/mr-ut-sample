package org.sivrim;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@RunWith(JUnitPlatform.class)
public class AverageCombinerTest {
	@Mock
	private Reducer<IntWritable, IntWritable, IntWritable, Occurrence>.Context mockContext;

	InOrder inOrder;

	AverageCombiner combiner;

	@BeforeEach
	public void setUp() throws Exception {
		combiner = new AverageCombiner();
		inOrder = inOrder(mockContext);
	}

	@Test
	public void testMap() throws IOException, InterruptedException {
		IntWritable[] input = { new IntWritable(1), new IntWritable(1), new IntWritable(1) };

		combiner.reduce(new IntWritable(5), Arrays.asList(input), mockContext);

		inOrder.verify(mockContext, times(1)).write(new IntWritable(1), new Occurrence(5, 3));

	}

	@AfterEach
	public void tearDown() throws Exception {
		verifyNoMoreInteractions(mockContext);
	}
}