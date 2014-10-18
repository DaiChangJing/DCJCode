package org.zkpk.hadoop.day0801.work1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result=new IntWritable();
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable value:arg1){
			sum+=value.get();
		}
		result.set(sum);
		arg2.write(arg0, result);
	}
	

}
