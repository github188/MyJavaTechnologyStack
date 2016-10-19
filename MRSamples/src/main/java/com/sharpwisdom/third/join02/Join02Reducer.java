package com.sharpwisdom.third.join02;

import com.sun.istack.internal.Nullable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wuys on 2016/8/16.
 */
public class Join02Reducer extends Reducer<IntWritable,User,NullWritable,Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<User> values, Context context) throws IOException, InterruptedException {
        User city = null;
        List<User> list = new ArrayList<>();
        for(User u:values){
            if(u.getFlag() == 0){
                city = u;
            }else{
                list.add(u);
            }
        }

        for(User u:list){
            u.setCityName(city.getCityName());
            context.write(NullWritable.get(), new Text(u.toString()));
        }
    }
}
