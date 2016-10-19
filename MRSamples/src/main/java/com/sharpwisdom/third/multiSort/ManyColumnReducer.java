package com.sharpwisdom.third.multiSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class ManyColumnReducer extends Reducer<IntPair,IntWritable,Text,Text> {

    @Override
    protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        StringBuffer sBuffer = new StringBuffer();
        for(IntWritable val:values){
            sBuffer.append(val.get() + ",");
        }
        int length = sBuffer.toString().length();
        String value = sBuffer.toString().substring(0,length-1);
        context.write(new Text(key.getFirstKey()),new Text(value));

    }
}
