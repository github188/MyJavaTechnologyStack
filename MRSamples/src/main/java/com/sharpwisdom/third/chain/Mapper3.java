package com.sharpwisdom.third.chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author wuys
 * @date 2016/8/18.
 */
public class Mapper3 extends Mapper<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        if(key.toString().length() < 3){
            context.write(key,value);
        }
    }
}
