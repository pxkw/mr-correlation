package org.myorg.mr.correlation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import org.junit.Test;
import org.myorg.mr.correlation.CorrelationMapper.CorrelationWriter;;
import org.myorg.mr.correlation.CorrelationMapper.CorrelationSalesWriter;
import org.myorg.mr.correlation.CorrelationMapper.CorrelationAverageWriter;


import java.io.IOException;

import static org.mockito.Mockito.*;

public class CorrelationMapperTest {

    @Test
    public void mapShouldUseCorrelationSalesWriterWhenFilePathIsSalesFile() throws IOException, InterruptedException {
        final CorrelationMapper mapper = spy(CorrelationMapper.class);
        final Mapper.Context ctx = mock(Mapper.Context.class);
        final Configuration conf = mock(Configuration.class);
        final FileSplit inputSplit = mock(FileSplit.class);

        when(ctx.getConfiguration()).thenReturn(conf);
        when(ctx.getInputSplit()).thenReturn(inputSplit);
        when(inputSplit.getPath()).thenReturn(new Path("/a/path"));
        when(conf.get(CorrelationJob.SALES_PATH)).thenReturn("/a/path");
        when(conf.get(CorrelationJob.AVERAGE_PATH)).thenReturn("/b/path");

        mapper.setup(ctx);

        verify(mapper).setWriter(isA(CorrelationSalesWriter.class));
    }

    @Test
    public void mapShouldUseCorrelationAverageWriterWhenFilePathIsAverageFile() throws IOException, InterruptedException {
        final CorrelationMapper mapper = spy(CorrelationMapper.class);
        final Mapper.Context ctx = mock(Mapper.Context.class);
        final Configuration conf = mock(Configuration.class);
        final FileSplit inputSplit = mock(FileSplit.class);

        when(ctx.getConfiguration()).thenReturn(conf);
        when(ctx.getInputSplit()).thenReturn(inputSplit);
        when(inputSplit.getPath()).thenReturn(new Path("/a/path"));
        when(conf.get(CorrelationJob.SALES_PATH)).thenReturn("/b/path");
        when(conf.get(CorrelationJob.AVERAGE_PATH)).thenReturn("/a/path");

        mapper.setup(ctx);

        verify(mapper).setWriter(isA(CorrelationAverageWriter.class));
    }


    @Test
    public void CorrelationSalesWriter_writeShouldWriteSalesOfAPairOfItems() throws IOException, InterruptedException {
        final CorrelationWriter writer = new CorrelationSalesWriter();
        final LongWritable key = new LongWritable(1L);
        final Text val = new Text("Shop01,12,28,38");
        final Mapper.Context ctx = mock(Mapper.Context.class);
        final Configuration conf = mock(Configuration.class);

        when(ctx.getConfiguration()).thenReturn(conf);
        when(conf.get(CorrelationJob.ITEM_NAMES)).thenReturn("ItemA,ItemB,ItemC");

        writer.write(key, val, ctx);

        verify(ctx).write(eq(new Text("ItemA,ItemB")), eq(new Text("ItemA=12")));
        verify(ctx).write(eq(new Text("ItemA,ItemC")), eq(new Text("ItemA=12")));
        verify(ctx).write(eq(new Text("ItemB,ItemC")), eq(new Text("ItemB=28")));
        verify(ctx).write(eq(new Text("ItemA,ItemB")), eq(new Text("ItemB=28")));
        verify(ctx).write(eq(new Text("ItemA,ItemC")), eq(new Text("ItemC=38")));
        verify(ctx).write(eq(new Text("ItemB,ItemC")), eq(new Text("ItemC=38")));
    }


    @Test
    public void CorrelationAverageWriter_writeShouldWriteAverageOfAnItem() throws IOException, InterruptedException {
        final CorrelationWriter writer = new CorrelationAverageWriter();
        final LongWritable key = new LongWritable(1L);
        final Text val = new Text("ItemB,59.6");
        final Mapper.Context ctx = mock(Mapper.Context.class);
        final Configuration conf = mock(Configuration.class);

        when(ctx.getConfiguration()).thenReturn(conf);
        when(conf.get(CorrelationJob.ITEM_NAMES)).thenReturn("ItemA,ItemB,ItemC");

        writer.write(key, val, ctx);

        verify(ctx).write(eq(new Text("ItemA,ItemB#a")), eq(new Text("ItemB=59.6")));
        verify(ctx).write(eq(new Text("ItemB,ItemC#a")), eq(new Text("ItemB=59.6")));
    }
}