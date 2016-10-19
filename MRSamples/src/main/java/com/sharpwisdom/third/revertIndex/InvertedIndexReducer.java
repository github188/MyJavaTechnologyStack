package com.sharpwisdom.third.revertIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sbBuffer = new StringBuffer();
        for(Text val:values){
            sbBuffer.append(val + ";");
        }
        context.write(key,new Text(sbBuffer.toString().substring(0,sbBuffer.toString().length() - 1)));
    }
}
