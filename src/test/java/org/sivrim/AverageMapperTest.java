package org.sivrim;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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
public class AverageMapperTest {
	@Mock
	private Mapper<LongWritable, Text, IntWritable, IntWritable>.Context mockContext;

	InOrder inOrder;

	AverageMapper mapper;

	@BeforeEach
	public void setUp() throws Exception {
		mapper = new AverageMapper();
		inOrder = inOrder(mockContext);
	}

	@Test
	public void testMap() throws IOException, InterruptedException {
		mapper.map(new LongWritable(1L), new Text("4"), mockContext);

		inOrder.verify(mockContext, times(1)).write(new IntWritable(4), new IntWritable(1));

		mapper.map(new LongWritable(1L), new Text("10"), mockContext);

		inOrder.verify(mockContext, times(1)).write(new IntWritable(10), new IntWritable(1));

	}

	@AfterEach
	public void tearDown() throws Exception {
		verifyNoMoreInteractions(mockContext);
	}
}