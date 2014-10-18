package org.zkpk.hadoop.day0801.work2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable result=new IntWritable();
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		int sum=0;
		int i=0;
		for(IntWritable value:arg1){
			sum+=value.get();
			i++;
		}
		result.set(sum/i);
		arg2.write(arg0, result);
		
	}
	
}
