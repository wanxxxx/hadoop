package com.fangxi.hadoop;

import com.fangxi.hadoop.entity.KeySortWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;

/**
 * 自定义reduce输入的排序方式
 */
public class DescSort_newWritable extends Configured implements Tool {
    private static class MyMapper extends Mapper<LongWritable, Text, KeySortWritable, Text> {
        private KeySortWritable outkey = new KeySortWritable();
        private String[] tmp = null;
        private Text outval = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, KeySortWritable, Text>.Context context) throws IOException, InterruptedException {
            tmp = value.toString().split(" ");
            if (tmp != null && tmp.length == 2) {
                outkey.setOutkey(Integer.parseInt(tmp[0]));
                outval.set(tmp[1]);
                context.write(outkey, outval);
            }
        }
    }
    //map的数据只要送到reduce（reduce任务只要存在），就会提前排序
    //只有设置job.setNumReduceTasks(0)，也就是关闭reduce，map的数据就不会自动排序
    //所以reduce里的输入是按key有序的


    //3.配置job
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "DescSort");
        //设置输入输出目录
        //设置输入输出数据的“格式化”类型
        Common.setSome(conf, job, args);


        //设置MR类
        job.setMapperClass(MyMapper.class);

        //设置要打包的主class （MapReduce所在内部类的容器container类——WordCount）
        //设置Map和Reduce类的输出类型（若相等则只设置Map类即可）
        //Map类输出（Reduce输入类型与之相等）
        job.setMapOutputKeyClass(KeySortWritable.class);
        job.setMapOutputValueClass(Text.class);


        return job.waitForCompletion(true) ? 1 : 0;
    }


    //程序执行方法

    public static void main(String[] args) {
        try {
            Date start = new Date();
            int result = ToolRunner.run(new DescSort_newWritable(), args);
            Common.setResult(start, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
