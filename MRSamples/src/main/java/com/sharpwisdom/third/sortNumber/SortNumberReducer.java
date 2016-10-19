package com.sharpwisdom.third.sortNumber;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 */
public class SortNumberReducer extends Reducer<IntWritable,Text,IntWritable,IntWritable> {

    private IntWritable num = new IntWritable(1);

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text val:values){
            context.write(num,key);
            num = new IntWritable(num.get() + 1);
        }
    }
}
