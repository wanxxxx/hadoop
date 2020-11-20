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
import java.util.Arrays;

/**
 * 冒号前是一个用户，冒号后是该用户的关注，求共同关注
 * 解决方案：
 * map:被关注者为key，粉丝为val
 * reduce:一个key里的所有val就是我们要求的用户
 */
public class FindSameFollow_Step1 extends Configured implements Tool {
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outval = new Text();
        private Text outkey = new Text();
        private String[] strs = null;
        private String fan = null;
        private String[] follows = null;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter("line_info", "total_line").increment(1L);
            strs = value.toString().split(":");
            if (strs.length > 1) {
                context.getCounter("line_info", "right_line").increment(1L);
                fan = strs[0];
                follows = strs[1].split(",");
                outval.set(fan);
                for (String follow :
                        follows) {
                    outkey.set(follow);
                    context.write(outkey, outval);
                }

            } else {
                context.getCounter("line_info", "error_line").increment(1L);
            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private Text outkey = new Text();
        private StringBuffer buffer = new StringBuffer();//存放共同关注者
        private String[] tmp = null;//存放共同关注者
        private Integer index = 0;
        int i = 0;
        int j = 0;

        @Override
        protected void reduce(Text follow, Iterable<Text> fans, Context context) throws IOException, InterruptedException {
            buffer.setLength(0);
            tmp = null;
            for (Text t :
                    fans) {
                buffer.append(t.toString() + " ");
            }
            buffer.setLength(buffer.length() - 1);//去掉最后一个空格
            tmp = buffer.toString().split(" ");
            Arrays.sort(tmp);
            for (i = 0; i < tmp.length; i++) {
                for (j = i + 1; j < tmp.length; j++) {
                    outkey.set(tmp[i] + "," + tmp[j]+":");
                    context.write(outkey, follow);
                    /*输出形式：B C:A
                     * B C 共同关注了A
                     * */
                }
            }

        }
    }


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
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 第二阶段 : 设置类


        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 第三阶段 : 提交job
        count = job.waitForCompletion(true) ? 0 : -1;
        Common.getCountOut(job);
        // 返回
        return count;
    }

    public static void main(String[] args) {
        try {
            // 调用run
            int count = ToolRunner.run(new FindSameFollow_Step1(), args);
            System.out.println(count == 0 ? "JOB OK!" : "JOB FAIL!");
            System.exit(count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
