package com.sharpwisdom.third.logurl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 */
public class LogUrlReducer extends Reducer<Text ,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val:values){
            sum += val.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
