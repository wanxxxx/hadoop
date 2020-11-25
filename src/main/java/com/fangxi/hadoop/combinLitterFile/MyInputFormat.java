package com.fangxi.hadoop.combinLitterFile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class MyInputFormat extends FileInputFormat<NullWritable,BytesWritable> {

    MyRecordReader myRecordReader=new MyRecordReader();
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        myRecordReader.initialize(split,context);
        //按文件读取，把整个文件读取后变成字节数组(重写前是按行读取)
        return myRecordReader;
    }

    //原文件是否可切割
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
