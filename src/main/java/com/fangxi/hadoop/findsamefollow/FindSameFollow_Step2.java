package com.fangxi.hadoop.findsamefollow;

import com.fangxi.hadoop.Common;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

/**
 * 冒号前是一个用户，冒号后是该用户的关注，求共同关注
 * 解决方案：
 * map:被关注者为key，粉丝为val
 * reduce:一个key里的所有val就是我们要求的用户
 */
public class FindSameFollow_Step2 extends Configured implements Tool {
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outval = new Text();
        private Text outkey = new Text();
        private String[] strs = null;
        private String fans = null;
        private String follow = null;

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            strs = value.toString().split(":");
            if (strs.length > 0) {
                fans = strs[0]+":";
                follow = strs[1].replace("\t", "");
                outkey.set(fans);
                outval.set(follow);
                context.write(outkey, outval);

            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private StringBuffer buffer = new StringBuffer();//存放共同关注者
        private Text outval=new Text();
        @Override
        protected void reduce(Text fans, Iterable<Text> follows, Context context) throws IOException, InterruptedException {
            buffer.setLength(0);
            for (Text t :
                    follows) {
                buffer.append(t.toString() + ",");
            }
            buffer.setLength(buffer.length() - 1);//去掉最后一个空格
            outval.set(buffer.toString());
            context.write(fans, outval);
            System.out.println(fans.toString() + outval.toString());
        }

    }




    @Override
    public int run(String[] args) throws Exception {
        // 创建方法的返回值
        int count = -1;

        // 创建hadoop的配置文件加载对象
        Configuration conf = this.getConf();
        // 创建job
        Job job = Job.getInstance(conf, "innerjoin");
        // 第一阶段 : 输入与输出的处理
        Common.setSome(conf, job, args);

        // 输入与输出的数据格式化类
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 第二阶段 : 设置类


        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        // 第三阶段 : 提交job
        count = job.waitForCompletion(true) ? 0 : -1;
        // 返回
        return count;
    }

    public static void main(String[] args) {
        try {
            // 调用run
            int count = ToolRunner.run(new FindSameFollow_Step2(), args);
            System.out.println(count == 0 ? "JOB OK!" : "JOB FAIL!");
            System.exit(count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
