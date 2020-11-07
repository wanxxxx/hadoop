package com.hnxy.mr;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

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
}
