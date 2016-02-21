package org.myorg.mr.correlation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CorrelationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
        Iterator<Text> val = values.iterator();

        val.next();
        val.next();

//        while(val.hasNext()) {
//            sum+=val.next().get();
//            num++;
//        }

//        final double avg = (double)sum/num;
//        ctx.write(key, new DoubleWritable(avg));
    }
}
