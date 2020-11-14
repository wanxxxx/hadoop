package com.fangxi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Date;

public class Common {
    public static void setResult(Date start, int result) {
        Date end = new Date();
        String msg = result == 1 ? "Job OK" : "JOB FAIL";
        System.out.println("\nTime spent " + (end.getTime() - start.getTime()) + " ms");
        System.out.println(msg);
        System.exit(result);
    }
    public static void getCountOut(Job job) throws IOException {
        /*----------------可选：获取counters-------------*/


        //获取counters
        Counters counters = job.getCounters();
        //输出所有counter
        for (CounterGroup cg : counters) {
            System.out.println("\t" + cg.getDisplayName());
            for (Counter c : cg) {
                System.out.println("\t\t" + c.getDisplayName() + "=" + c.getValue());

            }
        }
        /*-------------------------------------*/
    }
    public static void setSome(Configuration conf,Job job, String[] args) throws IOException{
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
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

    }
}
