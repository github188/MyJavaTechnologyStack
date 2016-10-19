package com.sharpwisdom.third.duplicateNumber;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/15.
 *
 * 原始数据:
 *  file1:
 *  2012-3-1 a
 *  2012-3-2 b
 *  2012-3-3 c
 *  2012-3-4 d
 *  2012-3-5 a
 *  2012-3-6 b
 *  2012-3-7 c
 *  2012-3-8 c
 *
 *  file2:
 *  2012-3-1 b
 *  2012-3-2 a
 *  2012-3-3 b
 *  2012-3-4 d
 *  2012-3-5 a
 *  2012-3-6 c
 *  2012-3-7 d
 *  2012-3-8 c
 *
 *  必须只有一个reducer
 */
public class DuplicateNumberMapper extends Mapper<LongWritable,Text,Text,Text> {

    private Text val = new Text("");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(line.trim().length() > 0){
            context.write(new Text(line.trim()),val);
        }
    }
}
