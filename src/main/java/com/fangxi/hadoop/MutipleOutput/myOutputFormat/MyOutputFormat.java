package com.fangxi.hadoop.MutipleOutput.myOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    private FileSystem fileSystem = null;
    private FSDataOutputStream goodOutputStream = null;
    private FSDataOutputStream badOutputStream = null;
    private MyRecordWriter myRecordWriter = null;


    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        goodOutputStream = fileSystem.create(new Path("file:///D:\\1\\out\\good-out.txt"));
        badOutputStream = fileSystem.create(new Path("file:///D:\\1\\out\\bad-out.txt"));
        myRecordWriter = new MyRecordWriter(goodOutputStream, badOutputStream);
        return myRecordWriter;
    }
}
