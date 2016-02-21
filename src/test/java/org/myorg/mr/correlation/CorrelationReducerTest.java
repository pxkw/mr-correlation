package org.myorg.mr.correlation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CorrelationReducerTest {

    @Test
    public void reduceShouldWriteCorrelation() throws IOException, InterruptedException {
        final CorrelationReducer reducer = new CorrelationReducer();
        final Text key = new Text("ItemA,ItemB");
        final Iterable<Text> val = Arrays.asList(
                new Text("40.8"),
                new Text("59.6"),
                new Text("12,28"),
                new Text("38,55"),
                new Text("50,87"),
                new Text("76,94") );
        Reducer.Context ctx = mock(Reducer.Context.class);

        reducer.reduce(key, val, ctx);

        verify(ctx).write( eq(new Text("ItemA,ItemB")), eq(new DoubleWritable(0.8654509489)) );
    }
}