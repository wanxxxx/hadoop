package com.fangxi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class DescSort extends Configured implements Tool {
    private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private IntWritable outkey = new IntWritable();
        private String[] tmp = null;
        private Text outval = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            tmp = value.toString().split(" ");
            if (tmp != null && tmp.length == 2) {
                outkey.set(Integer.parseInt(tmp[0]));
                outval.set(tmp[1]);
                context.write(outkey, outval);
            }
        }
    }
    //map的数据只要送到reduce（reduce任务只要存在），就会提前排序
    //只有设置job.setNumReduceTasks(0)，也就是关闭reduce，map的数据就不会自动排序
    //所以reduce里的输入是按key有序的

    private static class MyReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Map<Integer, String> map = new HashMap<>();
        private IntWritable outkey = new IntWritable();
        private Text outval = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)  {
            for (Text t :
                    values) {
                map.put(key.get(), t.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            ListIterator<Map.Entry<Integer, String>> li = new ArrayList<>(map.entrySet()).listIterator(map.size());
            //cleanup的输出不会自动排序
            while (li.hasPrevious()) {
                Map.Entry<Integer, String> entry = li.previous();
                outkey.set(entry.getKey());
                outval.set(entry.getValue());
                context.write(outkey, outval);
            }
        }
    }


    //3.配置job
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "DescSort");
        //设置输入输出目录
        //设置输入输出数据的“格式化”类型
        Common.setSome(conf,job,args);


        //设置MR类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置要打包的主class （MapReduce所在内部类的容器container类——WordCount）
        job.setJarByClass(DescSort.class);

        //设置Map和Reduce类的输出类型（若相等则只设置Map类即可）
        //Map类输出（Reduce输入类型与之相等）
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 1 : 0;
    }


    //程序执行方法

    public static void main(String[] args) {
        try {
            Date start = new Date();
            int result = ToolRunner.run(new DescSort(), args);
            Common.setResult(start, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
