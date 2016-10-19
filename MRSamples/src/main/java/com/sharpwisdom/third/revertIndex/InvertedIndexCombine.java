package com.sharpwisdom.third.revertIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class InvertedIndexCombine extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(Text val:values){
            sum += Integer.valueOf(val.toString());
        }

        String[] arr = key.toString().split(":");
        context.write(new Text(arr[0]),new Text(arr[1] + ":" + sum));

    }
}
