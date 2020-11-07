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
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;

public class Census extends Configured implements Tool {

    /*********************先配置**********************/
    //1.配置自己的map
    /*输入：LongWirtable——偏移量，Text——每行的数据*/
    /*输出：Text——每一个数据，IntWritable——对应的数量*/
    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        /*在map函数外定义需要用到的变量
         * 内存的使用是编写MapReduce程序时唯一要关心的问题，一定要严格控制内存使用*/
        private Text outkey = new Text();
        private IntWritable outval = new IntWritable();
        private String tmpvalue = null;
        private String[] tmp = null;

        //key——一行数据偏移量//
        //value——一行数据
        //context——从map到reduce的上下文对象*/
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拆分value
            //若文件为GBK编码，通过以下方法处理不会乱码（因为mr默认是UTF-8编码）
            tmpvalue = new String(value.getBytes(), 0, value.getLength(), "GBK");

            tmp = tmpvalue.split(" ");
            /*判断第一列是否为数字*/
            Pattern pattern = Pattern.compile("[0-9]*");
            /*判断第2 3 列是否为汉字*/
            String reg = "[\\u4e00-\\u9fa5]+";

            //拆分有效
            if (tmp != null && tmp.length == 3
                    && pattern.matcher(tmp[0]).matches()/*判断第一列是否为数字*/
                    && tmp[1].matches(reg) && tmp[2].matches(reg)/*判断第2 3 列是否为汉字*/
                    && tmp[2].substring(tmp[2].length() - 1).equals("市")/*判断第三列是否为xx市*/
                    && Arrays.toString(tmp).replace("[", "").replace("]", "").length() > 0) {
                context.getCounter("居民信息", tmp[2]).increment(1L);
                outkey.set(tmp[2]);
                int a = Integer.valueOf(tmp[0].replace("[", "").replace("]", ""));
                outval.set(a);
                context.write(outkey, outval);
            }

        }

    }

    //2.配置自己的reduce
    /*输入：map的输出*/
    /*输出：合并后的数量可能很大，所以用Long类型*/
    private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable outval = new IntWritable();


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {

            context.write(key, outval);
            System.out.print("户籍为" + key + "的居民编号有：");
            for (IntWritable i : values) {
                System.out.print(i.get() + " ");
            }
            System.out.println();

        }
    }

    //3.配置job
    public int run(String[] args) throws Exception {
        int count = -1;
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "census");
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
        job.setJarByClass(Census.class);

        //设置Map和Reduce类的输出类型（若相等则只设置Map类即可）
        //Map类输出（Reduce输入类型与之相等）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        //设置输入输出数据的“格式化”类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        count = job.waitForCompletion(true) ? 1 : 0;
        /*----------------可选：获取counters-------------*/
        Common.getCountOut(job);
        /*-------------------------------------*/
        //返回值

        return count;
    }


    //程序执行方法

    public static void main(String[] args) {
        try {
            Date start = new Date();
            int result = ToolRunner.run(new Census(), args);
            Common.setResult(start, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
