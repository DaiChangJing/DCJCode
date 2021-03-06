package org.zkpk.hadoop.day0802;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
   sogou500w每个用户搜索次数统计
 * 
 * @author acer
 *
 */
public class Main {
	public static void main(String []args) throws Exception{
		//1 args
		if(args.length!=2)
			throw new Exception("You must input 2 arguments:<in><out>");
		//2 create and run job
		Job job=new Job(new Configuration(),"UID Count");
		job.setJarByClass(Main.class);
		job.setMapperClass(LineSplitMapper.class);
		job.setReducerClass(SumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
