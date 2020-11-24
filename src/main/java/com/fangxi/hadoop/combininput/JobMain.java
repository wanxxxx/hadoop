package com.fangxi.hadoop.combininput;

import com.fangxi.hadoop.Common;
import com.fangxi.hadoop.findsamefollow.FindSameFollow_Step1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class JobMain extends Configuration implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 创建方法的返回值
        int count = -1;

        // 创建hadoop的配置文件加载对象
        Configuration conf = this.getConf();
        // 创建job
        Job job = Job.getInstance(conf, "fsf");
        // 第一阶段 : 输入与输出的处理
        Common.setSome(conf, job, args);

        // 输入与输出的数据格式化类
        job.setInputFormatClass(MyInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 第二阶段 : 设置类


        job.setMapperClass();
        job.setReducerClass();
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 第三阶段 : 提交job
        count = job.waitForCompletion(true) ? 0 : -1;
        Common.getCountOut(job);
        // 返回
        return count;
}
