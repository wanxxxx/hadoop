package com.fangxi.hadoop;

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
import java.util.*;

public class WordCountTop5_Cleanup extends Configured implements Tool {

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
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拆分value
            //若文件为GBK编码，通过以下方法处理不会乱码（因为mr默认是UTF-8编码）

            tmp = value.toString().split("\t| ");

            context.getCounter("line_info", "total_line").increment(1L);
            //拆分有效
            if (tmp != null && tmp.length > 0 && Arrays.toString(tmp).replace("[", "").replace("]", "").length() > 0) {
                context.getCounter("line_info", "right_line").increment(1L);
                for (String s : tmp) {
                    outkey.set(s);
                    outval.set(1);
                    context.write(outkey, outval);
                }
            } else {
                context.getCounter("line_info", "error_line").increment(1L);
            }


        }

    }

    //2.配置自己的reduce
    /*输入：map的输出*/
    /*输出：合并后的数量可能很大，所以用Long类型*/
    private static class MyReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        private LongWritable outval = new LongWritable();
        private Text outkey = new Text();
        private Long tmp = 0L;
        Map<String, Long> wcmap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context)  {

            // 清空前一次的累加记录
            tmp = 0L;
            // 循环当前的数据
            for (IntWritable i : values) {
                tmp += i.get();
            }
            wcmap.put(key.toString(),tmp);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //给map排序

            LinkedList<Map.Entry<String, Long>> list = new LinkedList<>(wcmap.entrySet());
            Collections.sort(list, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));

            //输出前几位

            for (int i = 0; i < 5; i++) {
                outkey.set(list.get(i).getKey());
                outval.set(list.get(i).getValue());
                System.out.println(outkey.toString()+"   "+outval);
                context.write(outkey, outval);
            }
        }
    }

    //3.配置job
    public int run(String[] args) throws Exception {
        int count = -1;
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wc");
        //设置输入输出目录
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);

        //自动删除输出目录
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
            System.out.println("Old path has already deleted");
        }


        //设置MR类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置要打包的主class （MapReduce所在内部类的容器container类——WordCount）
        job.setJarByClass(WordCountTop5_Cleanup.class);

        //设置Map和Reduce类的输出类型（若相等则只设置Map类即可）
        //Map类输出（Reduce输入类型与之相等）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //Reduce类输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输入输出数据的“格式化”类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        count = job.waitForCompletion(true) ? 1 : 0;
        /*----------------可选：获取counters-------------*/
        //获取counters
        Counters counters = job.getCounters();
        //获取想要的组
        CounterGroup counterGroup = counters.getGroup("line_info");

        /*-------------------------------------*/
        //返回值

        return count;
    }


    //程序执行方法

    public static void main(String[] args) {
        try {
            Date start = new Date();
            int result = ToolRunner.run(new WordCountTop5_Cleanup(), args);
            Common.setResult(start, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
