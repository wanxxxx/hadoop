package com.fangxi.hadoop.combinLitterFile;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    private Text outkey = new Text();

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        //获取文件名即为outkey
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String path = fileSplit.getPath().toString();
        String filename=path.substring(path.lastIndexOf("/")+1);
        outkey.set(fileSplit.getPath().getName());

        context.write(outkey, value);
    }
}
