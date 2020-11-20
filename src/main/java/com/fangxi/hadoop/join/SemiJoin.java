package com.fangxi.hadoop.join;

import com.fangxi.hadoop.Common;
import com.fangxi.hadoop.entity.AreaIfoWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
/*运行前要加上参数
-Dmapreduce.job.cache.files=file:/C:/Users/f/Desktop/data/semijoin/area/file1 C:\Users\f\Desktop\data\semijoin\person C:\Users\f\Desktop\data\semijoin\out
* */
public class SemiJoin extends Configured implements Tool {
    private static class MyMapper extends Mapper<LongWritable, Text, AreaIfoWritable, NullWritable> {
        private Map<Integer, String> cacheMap = new HashMap<>();
        private AreaIfoWritable outkey = new AreaIfoWritable();
        private String[] tmp = null;
        private NullWritable outval=NullWritable.get();
        private String tmparea=null;
        /*把*/
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            String path = cacheFiles[0].getPath().toString();
            String filename = path.substring(path.lastIndexOf("/") + 1);
            System.out.println(filename);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "UTF-8"));
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                tmp = line.split(" ");
                if (tmp != null && tmp.length == 2) {
                    cacheMap.put(Integer.parseInt(tmp[0]), tmp[1]);
                }
            }
            System.out.println("缓存数据大小" + cacheMap.size());
            bufferedReader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            tmp = value.toString().split(" ");
            tmparea=cacheMap.get(Integer.parseInt(tmp[0]));
            if (tmp != null && tmp.length == 3 ) {
                outkey.setId(Integer.parseInt(tmp[0]));
                outkey.setArea(tmparea);
                outkey.setYear(Integer.parseInt(tmp[1]));
                outkey.setCount(Integer.parseInt(tmp[2]));
                System.out.println(outkey.toString());
                context.write(outkey, outval);
            }
        }
    }



    //3.配置job
    @Override
    public int run(String[] args) throws Exception {
        int count = -1;
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "semijoin");
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
        job.setNumReduceTasks(0);

        //设置要打包的主class （MapReduce所在内部类的容器container类——WordCount）
        job.setJarByClass(SemiJoin.class);

        //设置Map和Reduce类的输出类型（若相等则只设置Map类即可）
        //Map类输出（Reduce输入类型与之相等）
        job.setMapOutputKeyClass(AreaIfoWritable.class);
        job.setMapOutputValueClass(NullWritable.class);


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
            int result = ToolRunner.run(new SemiJoin(), args);
            Common.setResult(start, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
