package com.sharpwisdom.third.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/12.
 */
public class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = 0;
        for(IntWritable val:values){
            maxValue = Math.max(maxValue,val.get());
        }
        context.write(key,new IntWritable(maxValue));
    }
}
