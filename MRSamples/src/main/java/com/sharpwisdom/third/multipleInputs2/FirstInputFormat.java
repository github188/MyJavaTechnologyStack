package com.sharpwisdom.third.multipleInputs2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by wuys on 2016/8/18.
 */
public class FirstInputFormat extends FileInputFormat<Text,FirstClass> {

    @Override
    public RecordReader<Text, FirstClass> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new FirstRecordReader();
    }
}
