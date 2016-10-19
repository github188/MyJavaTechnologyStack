package com.sharpwisdom.third.revertIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by wuys on 2016/8/17.
 *
 * Combine过程
 *
 *      经过map方法处理后,Combine过程将key值相同的value值累加，得到一个单词在文档中的词频。
 *
 * 需要解决的问题:
 *      本实例中设计的倒排索引在文件数量上没有限制，但是单词文件不宜过大(具体大小与HDFS块大小及相关配置有关)，要保证每个文件一个split。否则由于
 *      Reduce过程没有进一步统计词频，最终结果可能会出现词频未统计完全的单词。可以通过重写InputFormat类将每个文件为一个split，避免上述情况，或者
 *      执行两次mapreduce。第一次用于统计词频，第二次用于生成倒排索引。除此之外，还可以利用复合键值对等实现包含更多信息的倒排索引。
 *
 *
 */
public class RevertIndexMapper extends Mapper<LongWritable,Text,Text,Text>{

    private String fileName;

    private final Text val = new Text("1");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(line.trim().length() > 0){
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                String keyTmp = tokenizer.nextToken() + ":" + fileName;
                context.write(new Text(keyTmp),val);
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        String tmp = split.getPath().toString();//  /user/data/r_file1.txt
        fileName = tmp.substring(tmp.indexOf("file"));
    }
}
