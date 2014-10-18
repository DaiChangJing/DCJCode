package org.zkpk.hadoop.day0801.work1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




/**
 * sogou500w关键字长度统计MR
 */
public class Main {
	public static void main(String [] args) throws Exception{
		if(args.length!=2)
			throw new Exception("you must input 2 arguments :<in><out>");
		Job job=new Job(new Configuration(),"keywords length");
		job.setJarByClass(Main.class);
		job.setMapperClass(KeyLengthMapper.class);
		job.setReducerClass(SumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
