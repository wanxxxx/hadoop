package com.fangxi.hadoop.MutipleOutput.myOutputFormat;

import com.fangxi.hadoop.Common;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

//将结果分区，输出到不同的文件夹，多目录输出
//用partition不能实现输出到不同的文件夹
public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 创建方法的返回值
        int count = -1;

        // 创建hadoop的配置文件加载对象
        Configuration conf = super.getConf();
        // 创建job
        Job job = Job.getInstance(conf, "多目录输出");
        // 第一阶段 : 输入与输出的处理

        // 输入与输出的数据格式化类
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(MyOutputFormat.class);

        Path in = new Path("file:///D:\\1\\input\\MutipleOutput");
        Path out = new Path("file:///D:\\1\\out");
        TextInputFormat.addInputPath(job, in);
        MyOutputFormat.setOutputPath(job, out);

        //自动删除输出目录
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
            System.out.println("Old path has already deleted");
        }

        // 第二阶段 : 设置
        job.setMapperClass(MyOutputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 第三阶段 : 提交job
        count = job.waitForCompletion(true) ? 1 : 0;
        //Common.getCountOut(job);
        // 返回
        return count;
    }

    public static void main(String[] args) {
        try {
            Date start = new Date();
            int result = ToolRunner.run(new JobMain(), args);
            Common.setResult(start, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
