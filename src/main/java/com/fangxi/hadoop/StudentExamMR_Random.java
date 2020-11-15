package com.fangxi.hadoop;

import com.fangxi.hadoop.entity.StudentInfoWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

/**
 * 自定义Writable类为key，
 * 通过Pationner随机分到4个区
 * 缺点：牺牲了我们自己的分区业务
 * */


public class StudentExamMR_Random extends Configured implements Tool {

    private static class MyMapper extends Mapper<LongWritable, Text, StudentInfoWritable, NullWritable> {

        // 定义map需要用到的变量
        private StudentInfoWritable outkey = new StudentInfoWritable();
        private NullWritable outval = NullWritable.get();
        private String[] strs = null;

        @Override
        protected void map(LongWritable key, Text value,
                           Mapper<LongWritable, Text, StudentInfoWritable, NullWritable>.Context context)
                throws IOException, InterruptedException {
            // 拆分数据
            strs = value.toString().split("\t");
            // 索引为 1 和 最后一个数据
            outkey.setName(strs[1]);
            outkey.setScore(Integer.parseInt(strs[strs.length - 1]));
            context.write(outkey, outval);
        }
    }
    private static class MyPartitioner extends Partitioner<StudentInfoWritable, NullWritable> {
        // 设置随机返回
        private Random r = new Random();
        @Override
        public int getPartition(StudentInfoWritable key, NullWritable value, int numPartitions) {
            // 返回
            return r.nextInt(numPartitions); // 4 0 1 2 3
        }

    }


    //3.配置job
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wc");
        //设置输入输出目录
        Common.setSome(conf, job, args);


        //设置MR类
        job.setMapperClass(MyMapper.class);
       job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(4);
        //设置要打包的主class （MapReduce所在内部类的容器container类——WordCount）
        job.setJarByClass(StudentExamMR_Random.class);

        //设置Map和Reduce类的输出类型（若相等则只设置Map类即可）
        //Map类输出（Reduce输入类型与之相等）
        job.setMapOutputKeyClass(StudentInfoWritable.class);
        job.setMapOutputValueClass(NullWritable.class);


        //设置输入输出数据的“格式化”类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 1 : 0;
    }


    //程序执行方法

    public static void main(String[] args) {
        try {
            Date start = new Date();
            int result = ToolRunner.run(new StudentExamMR_Random(), args);
            Common.setResult(start, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
