package org.myorg.mr.correlation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class CorrelationMapper extends Mapper<LongWritable,Text,Text,Text>{

    private CorrelationWriter writer;

    @Override
    protected void setup(Context ctx) {

        Configuration conf = ctx.getConfiguration();
        String filePathString = ((FileSplit) ctx.getInputSplit()).getPath().toString();
        if(filePathString.equals(conf.get(CorrelationJob.SALES_PATH))) {
            setWriter(new CorrelationSalesWriter());
        }
        else if(filePathString.equals(conf.get(CorrelationJob.AVERAGE_PATH))){
            setWriter(new CorrelationAverageWriter());
        } else {
            throw new AssertionError("filePath is invalid: "+filePathString);
        }
    }

    public void setWriter(CorrelationWriter writer) {
        this.writer = writer;
    }

    interface CorrelationWriter {
        void write(LongWritable key, Text val, Context ctx) throws IOException, InterruptedException;
    }

    static class CorrelationSalesWriter implements CorrelationWriter {
        public void write(LongWritable key, Text val, Context ctx) throws IOException, InterruptedException {
            String[] itemNames = ctx.getConfiguration().get(CorrelationJob.ITEM_NAMES).split(",");
            String[] token = val.toString().split(",");
            for( int i=1; i<token.length; i++ ) {
                for ( int j=i+1; j<token.length; j++ ) {
                    String item1 = itemNames[i-1];
                    String item2 = itemNames[j-1];
                    int sales1 = Integer.parseInt(token[i]);
                    int sales2 = Integer.parseInt(token[j]);
                    ctx.write(new Text(item1+","+item2), new Text(item1+"="+sales1));
                    ctx.write(new Text(item1+","+item2), new Text(item2+"="+sales2));
                }
            }
        }
    }

    static class CorrelationAverageWriter implements CorrelationWriter {
        public void write(LongWritable key, Text val, Context ctx) throws IOException, InterruptedException {
            String[] itemNames = ctx.getConfiguration().get(CorrelationJob.ITEM_NAMES).split(",");
            String[] token = val.toString().split(",");
            String itemName = token[0];
            double itemValue = Double.parseDouble(token[1]);

            for( int i=0; i<itemNames.length; i++ ){
                String anotherName = itemNames[i];

                Text outKey;
                int comparedVal = itemName.compareTo(anotherName);
                if(comparedVal<0) {
                    outKey = new Text(itemName+","+anotherName+"#a");
                } else if(comparedVal>0) {
                    outKey = new Text(anotherName+","+itemName+"#a");
                } else {
                    continue;
                }

                Text outVal = new Text(itemName+"="+itemValue);
                ctx.write(outKey, outVal);
            }
        }
    }

    @Override
    public void map(LongWritable key, Text val, Context ctx) throws IOException, InterruptedException {
        this.writer.write(key, val, ctx);
    }
}