package com.fangxi.hadoop.MutipleOutput.hainiu;

import com.fangxi.hadoop.Common;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;

public class MutipleOutput extends Configured implements Tool {
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        /*在map函数外定义需要用到的变量
         * 内存的使用是编写MapReduce程序时唯一要关心的问题，一定要严格控制内存使用*/
        private Text outval = new Text();
        private String[] str = null;
        private String tmp = null;
        private Text outkey = new Text();

        //key——一行数据偏移量//
        //value——一行数据
        //context——从map到reduce的上下文对象*/
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拆分value
            str = value.toString().split(" ");
            //拆分有效
            tmp = str[11];
            if (tmp != null ) {
                if (tmp.equals("0") ||tmp.equals("1")) {
                    //好中评
                    outkey.set("good");
                    context.write(outkey, value);
                }
               else if (tmp.equals("2")) {
                    outkey.set("bad");
                    context.write(outkey, value);
                }
            }


        }


    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private Text outval = new Text();
        private Text first = new Text();
        private Double extremum = null;
        private Double tmpval = null;

        private MultipleOutputs<Text, Text> outputs = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputs = new MultipleOutputs<>(context);
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            System.out.println("<----------reduce--------->");
            for (Text t :
                    values) {

                //多目录输出结果文件
                outputs.write(key, t, key.toString() + "/");
                context.write(key, t);
            }


        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }
    }


    //3.配置job
    @Override

    public int run(String[] args) throws Exception {
        int count = -1;
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wc");

        //设置输入输出数据的“格式化”类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        //设置输入输出目录
        Path in = new Path("file:///D:\\1\\input\\MutipleOutput");
        Path out = new Path("file:///D:\\1\\out");
        TextInputFormat.addInputPath(job, in);
        SequenceFileOutputFormat.setOutputPath(job, out);
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
        job.setJarByClass(MutipleOutput.class);

        //设置Map和Reduce类的输出类型（若相等则只设置Map类即可）
        //Map类输出（Reduce输入类型与之相等）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        count = job.waitForCompletion(true) ? 1 : 0;
        /*----------------可选：获取counters-------------*/
        //Common.getCountOut(job);

        /*-------------------------------------*/
        return count;
    }


    //程序执行方法

    public static void main(String[] args) {
        try {
            Date start = new Date();
            int result = ToolRunner.run(new MutipleOutput(), args);
            Common.setResult(start, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
