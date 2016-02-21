package org.myorg.mr.error;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/*
 In -> Out
 ```
 "Shop,ItemA,ItemB,ItemC" -> (no output)
 "Shop01,12,28,38" -> {"ItemA,ItemB,ItemC":"12,28,38"}
 "Shop02,38,35,48" -> {"ItemA,ItemB,ItemC":"38,35,48"}
 "Shop03,28,55,24" -> ...
 ```
 */
public class ErrorMapperTest {

    @Test
    public void mapShouldWriteErrors() throws IOException, InterruptedException {
        final ErrorMapper mapper = new ErrorMapper();
        final LongWritable key = new LongWritable(1L);
        final Text val = new Text("Shop01,12,28,38");
        Mapper.Context ctx = mock(Mapper.Context.class);

        mapper.setup(ctx);

        mapper.map(key, val, ctx);

        verify(ctx).write(eq(new Text("ItemA,ItemB,ItemC")), eq(new Text("12,28,38")));
    }
}
