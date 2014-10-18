package org.zkpk.hadoop.day0801.work2;

//sogou500w关键字平均长度

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	public static void main(String [] args) throws Exception{
		if(args.length!=2)
			throw new Exception ("you must input 2 arguments:<in><out>");
		
		Job job=new Job();
		job.setJarByClass(Main.class);
		job.setMapperClass(PingMapper.class);
		job.setReducerClass(PingReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
