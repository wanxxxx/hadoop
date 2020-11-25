package com.fangxi.hadoop.combinLitterFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private Configuration conf = null;
    private FileSplit fileSplit = null;
    private boolean processed = false;
    FileSystem fileSystem = null;
    FSDataInputStream inputStream = null;

    private BytesWritable bytesWritable = null;

    public MyRecordReader() {
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //获取文件切片
        fileSplit = (FileSplit) inputSplit;
        //获取Configuration对象
        TaskAttemptContext context;
        conf = taskAttemptContext.getConfiguration();
    }

    /*获取key value
     * key    NullWritable
     * value  ByteWritable*/
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //若文件没被读取完整或者没读取，则继续读取
        if (!processed) {
            //1.获取原文件字节流
            //1.1获取源文件的文件系统
            fileSystem = FileSystem.get(conf);
            //1.2获取文件输入流
            inputStream = fileSystem.open(fileSplit.getPath());
            //2.读取之，放在字节数组byte[]
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            IOUtils.readFully(inputStream, bytes, 0, (int) fileSplit.getLength());
            //3.将字节数组封装为ByteWritable
            bytesWritable = new BytesWritable();
            bytesWritable.set(bytes, 0, (int) fileSplit.getLength());
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        fileSystem.close();

    }


}
