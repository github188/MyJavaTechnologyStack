package com.sharpwisdom.third.logurl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/16.
 * 需求:根据tomcat日志计算url访问情况
 *      要求:区分统计GET和POST URL访问量
 *      结果为:访问方式、URL、访问量
 * 原始数据:
 * 127.0.0.1 - - [03/Jul/2016:23:36:38 +0800] 'GET /course/detail/3.htm HTTP/1.0" 200 38435 0.038
 * 172.16.75.27 - - [03/Jul/2016:23:36:38 +0800] 'GET /course/detail/3.htm HTTP/1.0" 200 38435 0.038
 * 127.0.0.1 - - [03/Jul/2016:23:36:38 +0800] 'POST /service/detail/3.htm HTTP/1.0" 200 38435 0.038
 */
public class LogUrlMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    IntWritable val = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if(line.length() > 0){
            String tmp = handleLog(line);
            context.write(new Text(tmp),val);
        }
    }

    private String handleLog(String log){
        String result = "";
        if(log.length() > 20){
            if(log.indexOf("GET") > 0){
                result = log.substring(log.indexOf("GET"),log.indexOf("HTTP/1.0")).trim();
            }else if(log.indexOf("POST") > 0){
                result = log.substring(log.indexOf("POST"),log.indexOf("HTTP/1.0")).trim();
            }
        }
        return result;
    }
}
