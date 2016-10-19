package com.sharpwisdom.third.chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wuys
 * @date 2016/8/18.
 */
public class OnlyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val:values){
            sum += val.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
