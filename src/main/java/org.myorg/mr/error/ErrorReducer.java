package org.myorg.mr.error;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

class ErrorReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
        int sum = 0, num = 0;
        Iterator<IntWritable> val = values.iterator();

        while(val.hasNext()) {
            sum+=val.next().get();
            num++;
        }

        final double avg = (double)sum/num;
        ctx.write(key, new DoubleWritable(avg));
    }
}
