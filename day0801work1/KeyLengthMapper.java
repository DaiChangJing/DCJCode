package org.zkpk.hadoop.day0801.work1;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KeyLengthMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text keylength =new Text();
	private IntWritable one=new IntWritable(1);

	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String [] arr=value.toString().split("\t",-1);
		if(arr.length==6){
			if(isChinese(arr[2])){
				String n=Integer.toString(arr[2].length());
				keylength.set(n);
				context.write(keylength, one);
			}
			else{
				String [] arr2=arr[2].split(" ",-1);
				String y= Integer.toString(arr2.length);
				keylength.set(y);
				context.write(keylength, one);
			}
		}
	}
	//ÅÐ¶ÏÖÐÎÄ×Ö·û
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
