package com.sharpwisdom.third.maxAndMin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 * 需求: 求最大值和最小值        select max(number),min(number) from table
 *
 * 1.数据准备
 * 102
 * 10
 * 39
 * 109
 * 200
 * 12
 * 3
 * 90
 *
 */
public class MaxAndMinMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

    private Text keyText = new Text("Key");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(line.trim().length() > 0){
            context.write(keyText,new LongWritable(Long.valueOf(line.trim())));
        }
    }
}
