package com.fangxi.hadoop.sort;

import com.fangxi.hadoop.entity.AccountWritable;
import com.fangxi.hadoop.util.SecondSortComparator;
import com.fangxi.hadoop.util.SecondSortGroupByComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 需求 : 根据用户名通过Partitier进行散列分区,
 * 如果用户名相同的情况下，通过自定义的Comparator按照消费金额进行二次倒序排序
 */
public class SecondSort extends Configured implements Tool {


    // 从默认的key val 整理自己key val
    private static class MyMapper extends Mapper<LongWritable, Text, AccountWritable, NullWritable> {

        private AccountWritable outkey = new AccountWritable();
        private NullWritable outval = NullWritable.get();
        private String[] strs = null;

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 拆分数据
            strs = value.toString().split(" ");
            if (strs != null && strs.length == 2) {
                outkey.setName(strs[0]);
                outkey.setCost(Integer.parseInt(strs[strs.length - 1]));
            }
            context.write(outkey, outval);
        }
    }

    private static class MyPartitioner extends Partitioner<AccountWritable, NullWritable> {
        @Override
        public int getPartition(AccountWritable key, NullWritable value, int numPartitions) {
            //根据名字进行散列分区
            int a= (key.getName().hashCode() & Integer.MAX_VALUE)%numPartitions;
            System.out.println("key.getName().hashCode() & Integer.MAX_VALUE)%numPartitions");
            System.out.println(key.getName()+":"+key.getName().hashCode()+"&"+Integer.MAX_VALUE+"  %"+numPartitions);
            System.out.println((key.getName().hashCode() & Integer.MAX_VALUE)+"%"+numPartitions+" ="+a);
            return a;
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
        Path in = new Path(args[0]); // 输入路径
        Path out = new Path(args[1]); // 输出路径
        // 保证输出路径是不存在
        FileSystem fs = FileSystem.get(conf); // 本地 HDFS FS --> conf --> 本地?HDFS?
        if (fs.exists(out)) {
            // hadoop fs -rm -r 递归删除
            fs.delete(out, true);
            System.out.println("旧的输出目录已经被删除!");
        }
        // 输入与输出的数据格式化类
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输入与输出的路径
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);


        // 第二阶段 : 设置MRCJ类
        job.setMapperClass(MyMapper.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setMapOutputKeyClass(AccountWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置分组规则
        job.setGroupingComparatorClass(SecondSortGroupByComparator.class);
        //设置组内排序（即二次排序）
        job.setSortComparatorClass(SecondSortComparator.class);
        job.setNumReduceTasks(2);


        // 第三阶段 : 提交job
        count = job.waitForCompletion(true) ? 0 : -1;
        // 返回
        return count;
    }

    public static void main(String[] args) {
        try {
            // 调用run
            int count = ToolRunner.run(new SecondSort(), args);
            System.out.println(count == 0 ? "JOB OK!" : "JOB FAIL!");
            System.exit(count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
