package com.sharpwisdom.third.multipleInputs;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class MultipleInputsReducer extends Reducer<Text,FlagStringDataType,NullWritable,Text> {

    private String delimiter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        delimiter = context.getConfiguration().get("delimiter",",");
    }

    @Override
    protected void reduce(Text key, Iterable<FlagStringDataType> values, Context context) throws IOException, InterruptedException {
        //user phone phone-desc
        String[] result = new String[3];//phone-desc,user,phone
        result[2] = key.toString();
        for(FlagStringDataType val:values){
            result[val.getFlag()] = val.getValue();
        }
        context.write(NullWritable.get(), new Text(result[1] + delimiter + result[2] + delimiter + result[0]));
    }
}
