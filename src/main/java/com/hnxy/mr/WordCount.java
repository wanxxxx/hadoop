package com.hnxy.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordCount extends Configured implements Tool {

    /*********************先配置**********************/
    //1.配置自己的map
    /*输入：LongWirtable——偏移量，Text——每行的数据*/
    /*输出：Text——每一个数据，IntWritable——对应的数量*/
    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        //key——一行数据偏移量//
        // value——一行数据
        // context——从map到reduce的上下文对象
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    //2.配置自己的reduce
    /*输入：map的输出*/
    /*输出：合并后的数量可能很大，所以用Long类型*/
    private static class MyRuducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    }

    //3.配置job
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wordcount");


        //1.设置要打包的主class （MapReduce所在内部类的容器container类——WordCount）
        //2.设置自己程序的MR
        //3.设置输入输出数据的格式化类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //4.设置MR泛型参数
        //5.设置输入输出位置
        return 0;
    }


    //程序执行方法

    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new WordCount(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
