package com.fangxi.hadoop.combinLitterFile;

import com.fangxi.hadoop.Common;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;
//合并小文件
public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 创建方法的返回值
        int count = -1;

        // 创建hadoop的配置文件加载对象
        Configuration conf = super.getConf();
        // 创建job
        Job job = Job.getInstance(conf, "合并小文件");
        // 第一阶段 : 输入与输出的处理

        // 输入与输出的数据格式化类
        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        Path in = new Path("file:///D:\\1\\input\\myInputformat_input");
        Path out = new Path("file:///D:\\1\\out\\myInputformat_out");
        MyInputFormat.addInputPath(job, in);
        SequenceFileOutputFormat.setOutputPath(job, out);

        //自动删除输出目录
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
            System.out.println("Old path has already deleted");
        }

        // 第二阶段 : 设置
        job.setMapperClass(SequenceFileMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        // 第三阶段 : 提交job
        count = job.waitForCompletion(true) ? 0 : -1;
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
