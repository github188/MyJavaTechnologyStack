package com.sharpwisdom.third.join02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 * 需求:将人员的地址id完善成为地址名称,输出格式要求:人员id,姓名,地址
 *
 * 1.原始数据
 * 人员ID     人员名称    地址ID
 * 1            张三      1
 * 2            李四      2
 *
 * 另一组为地址信息:
 * 地址ID     地址名称
 * 1            北京
 * 2            上海
 *
 *
 */
public class Join02Mapper extends Mapper<LongWritable,Text,IntWritable,User>{

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\t");
        if(arr.length ==2){//城市信息
            User user = new User();
            user.setCityNo(arr[0]);
            user.setCityName(arr[1]);
            user.setFlag(0);
            context.write(new IntWritable(Integer.valueOf(arr[0])),user);
        }else{//人员信息
            User user = new User();
            user.setUserNo(arr[0]);
            user.setUserName(arr[1]);
            user.setCityNo(arr[2]);
            user.setFlag(1);
            context.write(new IntWritable(Integer.valueOf(arr[2])),user);
        }
    }
}
