package org.myorg.mr.error;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ErrorReducerTest {
    @Test
    public void reduceShouldWriteItemAndAverage() throws IOException, InterruptedException {
        ErrorReducer reducer = new ErrorReducer();
        final Text key = new Text("ItemA");
        final Iterable<IntWritable> values = Arrays.asList(new IntWritable(12), new IntWritable(38));
        Reducer.Context ctx = mock(Reducer.Context.class);

        reducer.reduce(key, values, ctx);

        verify(ctx).write(eq(new Text("ItemA")), eq(new DoubleWritable(25)));

    }
}