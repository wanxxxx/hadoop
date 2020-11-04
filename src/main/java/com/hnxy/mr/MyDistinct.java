package com.hnxy.mr;

import java.io.IOException;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyDistinct extends Configured implements Tool {
	
	// 自己的map
	private static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		// 在map函数外部定义需要用到的变量!!!!!!!!!!!!!!!!
		// 内存的使用是我们在编写MAPREDUCE程序的时候唯一要关心的问题,一定要严格控制内存的使用
		private Text outkey = new Text();
		private NullWritable outval = NullWritable.get();
		private String[] tmp = null;
		// key --> 一行数据的数据偏移量
		// value --> 一行数据
		// context --> 从 map 到 reduce 阶段的上下文对象
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// 将val 进行拆分
			tmp = value.toString().split(" "); // 赵文明 赵文明 赵文明 --> [赵文明,赵文明,赵文明]
			// 判断数据切分是否有效
			if(null != tmp && tmp.length > 0){ // 有
				// 循环
				for (String s : tmp) { // [赵文明,赵文明,赵文明]
					outkey.set(s); // 赵文明
					// 赵文明 : 1 --> key val
					context.write(outkey, outval);
				}
			}
		}
	}
	
	// 自己的reduce
	private static class MyReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		
		// 定义需要用到的变量
		private NullWritable outval = NullWritable.get();
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			// 进行输出设置
			context.write(key, outval);
		}
	}
	

	// JOB --> job配置
	public int run(String[] args) throws Exception {
		
		// 设定方法的返回值
		int count = -1;
		
		// 组织自己的job
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wordcount");
		// job在组织的时候 需要几个关键设置
		// 自动删除输出目录
		FileSystem fs = FileSystem.get(conf);
		Path op = new Path(args[1]);
		if(fs.exists(op)){
			// hadoop fs -rm -r
			fs.delete(op, true);
			System.out.println("Old output path is deleted!");
		}
		// 1. 要设置一下 自己要打包的主类 (MapReduce所在内部类的容器类 MyWordCount)
		
		
		job.setJarByClass(MyDistinct.class);
		
		
		// 2. 设置你自己程序的MR 设置MR泛型参数
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// 3. 设置一下你的输入与输出数据的格式化类
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class); 
		// 4. 本次job的输入与输出位置
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, op);
		// 运行这个任务 1 成功 0 失败
		count = job.waitForCompletion(true)?1:0; // 一致等待这个任务完成再返回结果
		// 返回
		return count;
	}
	// 程序执行方法
	public static void main(String[] args) {
		// args 配置 数据的输入与输出的位置
		try {
			System.exit(ToolRunner.run(new MyDistinct(), args));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
