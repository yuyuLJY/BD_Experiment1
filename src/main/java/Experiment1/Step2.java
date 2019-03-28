package Experiment1;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Step2 {
	public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
		value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
		e.printStackTrace();}
		return new Text(value);
	}
	
	public static class step2StandardMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			//value = transformTextToUTF8(value,"GBK");
			String[] splitResult = value.toString().split("\\|");//需要转意
			System.out.println("Map："+value.toString());
			context.write(new Text(splitResult[10]), new Text(value.toString()));//<"doctor",整条信息>
		}
	}
	
	public static class step2StandardReducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  for(Text value:values) {
				  //System.out.println(value.toString());
				  String[] saveSplitResult = value.toString().split("\\|");
				  //[8.1461259, 11.1993265]
				  //[56.5824856, 57.750511]
				  //System.out.printf("%s %s\n",saveSplitResult[1],saveSplitResult[2]);
				  if(Double.valueOf(saveSplitResult[1])>8.1461259 && Double.valueOf(saveSplitResult[1])<11.1993265 && 
					  Double.valueOf(saveSplitResult[2])>56.5824856 && Double.valueOf(saveSplitResult[2])<57.750511) {
					  System.out.println("reducer:"+value.toString());
					  context.write(new Text(value.toString()),new Text());
				 }
			  }
		  }
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(Main.config(),"step2");
        String input = path.get("Step2Input");
        String output = path.get("Step2Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(Step2.class);
        job.setMapperClass(step2StandardMapper.class);
        job.setReducerClass(step2StandardReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
}
