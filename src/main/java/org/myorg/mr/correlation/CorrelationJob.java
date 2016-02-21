package org.myorg.mr.correlation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.myorg.mr.error.ErrorJob;

import java.io.IOException;

public class CorrelationJob {

    public static final String SALES_PATH = "salesPath";
    public static final String AVERAGE_PATH = "averagePath";
    public static final String CORRELATION_PATH = "correlationPath";
    public static final String ITEM_NAMES = "itemNames";

    /**
     * @param args 0:salesPath, 2:correlationPath
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        ErrorJob.main(args);

        Configuration conf = new Configuration();
        conf.set(SALES_PATH, args[0]);
        conf.set(AVERAGE_PATH, args[1]);
        conf.set(CORRELATION_PATH, args[2]);
        conf.set(ITEM_NAMES, "ItemA,ItemB,ItemC"); // 手抜き
        Job job = Job.getInstance(conf, "correlation");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(CorrelationMapper.class);
        job.setReducerClass(CorrelationReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setPartitionerClass(CorrelationkeyPartitioner.class);
        job.setGroupingComparatorClass(CorrelationKeyGroupingComparator.class);
        job.setSortComparatorClass(CorrelationKeySortingComparator.class);

        job.setJarByClass(CorrelationJob.class);
        job.waitForCompletion(true);
    }

    public class CorrelationkeyPartitioner extends HashPartitioner<Text,Text> {
        @Override
        public int getPartition(Text key, Text val, int numReduceTasks) {
            // TODO
        return super.getPartition(key, val, numReduceTasks);
        }
    }

    private static abstract class CorrelationKeyComparator implements RawComparator<Text> {
        private final Text text1 = new Text();
        private final Text text2 = new Text();
        private DataInputBuffer buffer = new DataInputBuffer();

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                buffer.reset(b1, s1, l1);
                text1.readFields(buffer);
                buffer.reset(b2, s2, l2);
                text2.readFields(buffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return compare(text1, text2);
        }

        @Override
        public abstract int compare(Text o1, Text o2);
    }


    private static class CorrelationKeyGroupingComparator extends CorrelationKeyComparator
            implements RawComparator<Text> {
        @Override
        public int compare(Text t1, Text t2) {
            return 0;
        }
    }

    private static class CorrelationKeySortingComparator extends CorrelationKeyComparator
            implements RawComparator<Text> {
        @Override
        public int compare(Text t1, Text t2) {
            return 0;
        }
    }

}
