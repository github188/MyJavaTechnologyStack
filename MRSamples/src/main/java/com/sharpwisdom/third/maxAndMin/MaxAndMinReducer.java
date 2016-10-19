package com.sharpwisdom.third.maxAndMin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 */
public class MaxAndMinReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;

        for(LongWritable val:values){
            if(val.get() > max){
                max = val.get();
            }
            if(val.get() < min){
                min = val.get();
            }
        }
        context.write(new Text("Max"),new LongWritable(max));
        context.write(new Text("Min"),new LongWritable(min));
    }
}
