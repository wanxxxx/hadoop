package com.hnxy.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WordCount extends Configured implements Tool {

    /*********************先配置**********************/
    //1.配置自己的map
    /*输入：LongWirtable——偏移量，Text——每行的数据*/
    /*输出：Text——每一个数据，IntWritable——对应的数量*/
    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        /*在map函数外定义需要用到的变量
         * 内存的使用是编写MapReduce程序时唯一要关心的问题，一定要严格控制内存使用*/
        private Text outkey = new Text();
        private IntWritable outval = new IntWritable();
        private String[] tmp = null;

        //key——一行数据偏移量//
        //value——一行数据
        //context——从map到reduce的上下文对象*/
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //拆分value
            tmp = value.toString().split(" ");

            context.getCounter("line_info","total_line").increment(1L);
            //拆分有效
            if (tmp != null && tmp.length > 0 && !tmp[0].equals("")) {
                context.getCounter("line_info","right_line").increment(1L);
                for (String s : tmp) {
                    outkey.set(s);
                    outval.set(1);
                    context.write(outkey, outval);
                }
            }
            else{
                context.getCounter("line_info","error_line").increment(1L);
            }


        }

    }

    //2.配置自己的reduce
    /*输入：map的输出*/
    /*输出：合并后的数量可能很大，所以用Long类型*/
    private static class MyRuducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        private LongWritable outval = new LongWritable();
        private Long tmp = 0L;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            // 清空前一次的累加记录

            tmp = 0L;
            // 循环当前的数据
            for (IntWritable i : values) {
                tmp+=i.get();
            }
            // 进行输出设置
            outval.set(tmp);
            context.write(key, outval);
            System.out.println("key :" + key + "; values :" + tmp);
        }
    }

    //3.配置job
    public int run(String[] args) throws Exception {
        int count = -1;//程序返回值

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wordcount");
        //自动删除输出目录
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[1]);
        if (fs.exists(path)) {
            fs.delete(path, true);
            System.out.println("Old path has already deleted");
        }

        //1.设置要打包的主class （MapReduce所在内部类的容器container类——WordCount）
        job.setJarByClass(WordCount.class);
        //2.设置自己程序的MR，MR泛型参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyRuducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //3.设置输入输出数据的格式化类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /*还可以这样设置*/
        /*conf.set("mapreduce.job.reduces","10");*/
        /*job.setNumReduceTasks(1);*/
        //4.设置输入输出位置
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //5.运行项目
        count = job.waitForCompletion(true) ? 1 : 0;
        //6.获取counters
        //获取counters
        Counters counters=job.getCounters();
        //获取想要的组
        CounterGroup counterGroup=counters.getGroup("line_info");
        //通过list存储key val
        /*List key=new ArrayList();
        List val=new ArrayList();*/
        List tt=new ArrayList();
        for (Counter tmp:
             counterGroup) {
/*            key.add(tmp.getDisplayName());
            val.add(tmp.getValue());*/
            tt.add(tmp.getName());
            tt.add(tmp.getValue());
        }
        System.out.println(tt.toString());
        System.out.println(counterGroup.findCounter("error_line").getDisplayName()+"   "+
                counterGroup.findCounter("error_line").getValue()
        );
        return count;
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
