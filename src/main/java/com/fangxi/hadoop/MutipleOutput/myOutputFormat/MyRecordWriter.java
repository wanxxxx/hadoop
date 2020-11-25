package com.fangxi.hadoop.MutipleOutput.myOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    private String[] str = null;
    private String tmp = null;
    FSDataOutputStream goodOutputStream = null;
    FSDataOutputStream badOutputStream = null;

    public MyRecordWriter() {
    }

    public MyRecordWriter(FSDataOutputStream goodOutputStream, FSDataOutputStream badOutputStream) {
        this.goodOutputStream = goodOutputStream;
        this.badOutputStream = badOutputStream;
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //获取每行的第九个字段（中差评标识符）
        str = text.toString().split(" ");
        tmp = str[11];
        if (Integer.parseInt(tmp) <= 1) {
            //好中评
            goodOutputStream.write(text.toString().getBytes());
            goodOutputStream.write("\r\n".getBytes());
        } else {
            badOutputStream.write(text.toString().getBytes());
            goodOutputStream.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(goodOutputStream);
        IOUtils.closeStream(badOutputStream);
    }


}
