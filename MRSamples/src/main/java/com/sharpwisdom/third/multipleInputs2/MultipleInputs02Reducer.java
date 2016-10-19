package com.sharpwisdom.third.multipleInputs2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wuys
 * @date 2016/8/18.
 */
public class MultipleInputs02Reducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text val:values){
            context.write(key,val);
        }
    }
}
