package com.sharpwisdom.third.duplicateNumber;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 */
public class DuplicateNumberReducer extends Reducer<Text,Text,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
