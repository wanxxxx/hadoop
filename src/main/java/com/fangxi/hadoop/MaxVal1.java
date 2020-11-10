package com.fangxi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MedicineWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;

public class MaxVal1 extends Configured implements Tool {
    private static class MyMapper extends Mapper<LongWritable, Text, Text, MedicineWritable> {
        /*在map函数外定义需要用到的变量
         * 内存的使用是编写MapReduce程序时唯一要关心的问题，一定要严格控制内存使用*/
        private Text outkey = new Text();
        private MedicineWritable outval = new MedicineWritable();
        private String[] tmp = null;
        private String maxname = null;
        private String tmpname = null;
        private Double maxval = null;
        private Double tmpval = null;

        //key——一行数据偏移量//
        //value——一行数据
        //context——从map到reduce的上下文对象*/
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拆分value
            tmp = value.toString().split("\t| ");

            //拆分有效
            if (tmp != null && tmp.length == 7) {
                tmpname = tmp[3];
                tmpval = Double.parseDouble(tmp[6].trim());
                if (maxval == null && maxname == null || maxval < tmpval) {
                    maxval = tmpval;
                    maxname = tmpname;
                }

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println(maxname + maxval);
            context.write(new Text(maxname), new MedicineWritable(maxval));
        }
    }

    //2.配置自己的reduce
    /*输入：map的输出*/
    /*输出：合并后的数量可能很大，所以用Long类型*/
    private static class MyReducer extends Reducer<Text, MedicineWritable, Text, MedicineWritable> {
        private String maxname = null;
        private Double maxval = null;
        private Double tmpval = null;

        @Override
        protected void reduce(Text key, Iterable<MedicineWritable> value, Context context) throws IOException, InterruptedException {
            tmpval = value.iterator().next().get();
            if (maxval == null && maxname == null || maxval < tmpval) {
                maxval = tmpval;
                maxname = key.toString();
            }
            //context.write(key, new DoubleWritable(Double.parseDouble(value.iterator().next())));
        }

        @Override
        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            System.out.println(maxname + maxval);
            context.write(new Text(maxname), new MedicineWritable(maxval));
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
        job.setJarByClass(MaxVal1.class);

        //设置Map和Reduce类的输出类型（若相等则只设置Map类即可）
        //Map类输出（Reduce输入类型与之相等）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MedicineWritable.class);
        //Reduce类输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MedicineWritable.class);

        //设置输入输出数据的“格式化”类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        count = job.waitForCompletion(true) ? 1 : 0;
        /*----------------可选：获取counters-------------*/
        //Common.getCountOut(job);

        /*-------------------------------------*/
        //返回值

        return count;
    }


    //程序执行方法

    public static void main(String[] args) {
        try {
            Date start = new Date();
            int result = ToolRunner.run(new MaxVal1(), args);
            Common.setResult(start, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
