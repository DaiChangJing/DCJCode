package org.zkpk.hadoop.day0801.work2;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PingMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text keys=new Text();
	private IntWritable one=new IntWritable();
	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String [] arr=value.toString().split("\t",-1);
		String s="sum";
		keys.set(s);
		if(arr.length==6){
			if(isChinese(arr[2])){
				int n=arr[2].length();
				one.set(n);
				context.write(keys, one);
			}
			else{
				String [] arr2=arr[2].split(" ",-1);
				int y= arr2.length;
				one.set(y);
				context.write(keys, one);
			}
		}
	}
	protected boolean isChinese(String str){
		boolean temp = false;
        Pattern p=Pattern.compile("[\u4e00-\u9fa5]"); 
        Matcher m=p.matcher(str); 
        if(m.find()){ 
            temp =  true;
        }
        return temp;
	}
	

}
