package com.sharpwisdom.third.join03;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/17.
 */
public class Join03Reducer extends Reducer<UserKey,User,NullWritable,Text> {

    @Override
    protected void reduce(UserKey key, Iterable<User> values, Context context) throws IOException, InterruptedException {
        User user = null;
        int num = 0;
        for(User u: values){
            if(num == 0){
                user = u;
                num ++;
            }else{
                u.setCityName(user.getCityName());
                context.write(NullWritable.get(),new Text(u.toString()));
            }
        }
    }
}
