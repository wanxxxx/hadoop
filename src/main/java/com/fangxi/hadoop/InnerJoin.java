package com.fangxi.hadoop;

import com.fangxi.hadoop.entity.AreaIfoWritable;
import com.fangxi.hadoop.entity.AreaIfoWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class InnerJoin extends Configured implements Tool {
    private static class MyMapper extends Mapper<LongWritable, Text, Text, AreaIfoWritable> {

        private Text outkey = new Text();
        private AreaIfoWritable outval = new AreaIfoWritable();
        private String[] tmp = null;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拆分value
            tmp = value.toString().split(" ");
            //拆分有效
            if (tmp != null && tmp.length == 2) {
                outkey.set(tmp[0]);
                outval.setArea(tmp[1]);
            } else if (tmp != null && tmp.length == 3) {
                outkey.set(tmp[0]);
                outval.setYear(Integer.parseInt(tmp[1]));
                outval.setCount(Integer.parseInt(tmp[2]));
            }
            System.out.println(outkey.toString() + " " + outval.toString());
            context.write(outkey, outval);
        }


    }


    private static class MyReducer extends Reducer<Text, AreaIfoWritable, Text, AreaIfoWritable> {
        private List<AreaIfoWritable> list = new LinkedList();
        private AreaIfoWritable tmp = null;
        private String areaOfList = null;

        @Override
        protected void reduce(Text key, Iterable<AreaIfoWritable> values, Context context) throws IOException, InterruptedException {
            list.clear();
            areaOfList = null;
            System.out.println("<----------reduce--------->");
            System.out.println(key.toString());
            for (AreaIfoWritable m : values) {
                System.out.println(m.toString());

                if (m.getArea().equals("")) {
                    tmp = new AreaIfoWritable();
                    tmp.changeTo(m);
                    list.add(tmp);
                } else {
                    areaOfList = m.getArea();
                }
            }
            if (areaOfList != null) {
                for (AreaIfoWritable l :
                        list) {
                    l.setArea(areaOfList);
                    context.write(key, l);
                    System.out.println(l.toString());
                }
            }

        }
    }


    //3.配置job
    @Override
    public int run(String[] args) throws Exception {
        int count = -1;
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wc");
        //设置输入输出目录
        Common.setInAndOut(conf, job, args);


        //设置MR类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //设置要打包的主class （MapReduce所在内部类的容器container类——WordCount）
        job.setJarByClass(InnerJoin.class);

        //设置Map和Reduce类的输出类型（若相等则只设置Map类即可）
        //Map类输出（Reduce输入类型与之相等）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AreaIfoWritable.class);


        //设置输入输出数据的“格式化”类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        count = job.waitForCompletion(true) ? 1 : 0;
        /*----------------可选：获取counters-------------*/
        //Common.getCountOut(job);

        /*-------------------------------------*/
        return count;
    }


    //程序执行方法

    public static void main(String[] args) {
        try {
            Date start = new Date();
            int result = ToolRunner.run(new InnerJoin(), args);
            Common.setResult(start, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
