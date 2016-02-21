package org.myorg.mr.error;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class ErrorMapper extends Mapper<LongWritable,Text,Text,Text>{

    private String[] itemNames = null;

    @Override
    protected void setup(Context ctx){
        itemNames = new String[] {"ItemA", "ItemB", "ItemC"};
    }

    @Override
    public void map(LongWritable key, Text val, Context ctx) throws IOException, InterruptedException {
        String keyStr = "";
        for(String itemName : itemNames) {
            keyStr += itemName + ",";
        }
        keyStr = keyStr.substring(0, keyStr.length()-1);

        final String[] tokens = val.toString().split(",");
        String valueStr = "";
        for( int i=1; i<tokens.length; i++ ){
            valueStr += tokens[i] + ",";
        }
        valueStr = valueStr.substring(0, valueStr.length()-1);

        ctx.write(new Text(keyStr), new Text(valueStr));

    }
}