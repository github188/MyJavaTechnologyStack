package com.sharpwisdom.third.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by wuys on 2016/8/16.
 */
public class TopnReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

    int  len;
    int[] top;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        len = context.getConfiguration().getInt("N",10);
        top = new int[len + 1];
    }

    private void add(int payment){
        top[0] = payment;
        Arrays.sort(top);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        add(Integer.valueOf(key.get()));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int i = len;i>0;i--){//不取第一个位置上的
            context.write(new IntWritable(len -i + 1),new IntWritable(top[i]));
        }
    }
}
