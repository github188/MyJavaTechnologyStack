package com.sharpwisdom.third.secondarySort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class SecondarySortReducer extends Reducer<IntPair,Text,NullWritable,Text> {

    private static Text SEP = new Text("----------------------------------");

    @Override
    protected void reduce(IntPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), SEP);
        for(Text val:values){
            context.write(NullWritable.get(),val);
        }
    }
}
