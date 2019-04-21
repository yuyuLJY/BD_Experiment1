package Experiment1;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step4_1 {
	/**
	 * 方法(1)先找出有？的式子
	 * (2)用平均数的方法来求均值
	 */
	
	
    static double hightRate = 1.04;
    static double lowRate = 0.96;
	public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
		value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
		e.printStackTrace();}
		return new Text(value);
	}
    
	public static class step4_1StandardMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\\|");//特殊字符
			if(splitResult[6].contains("?")) {
				context.write(new Text(), value);
				System.out.println("rating缺失："+value.toString());
			}
			
			//context.write(new Text(), value);//<"ID",整条信息>
		}
	}
	
	public static class step4_1StandardReducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  System.out.println("reducer-------------");
			  for(Text value:values) {  
				  context.write(new Text(value.toString()),new Text());
			  }
		  }
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(Main.config(),"step4_1");
        String input = path.get("Step4_1Input");
        String output = path.get("Step4_1Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(Step4_1.class);
        job.setMapperClass(step4_1StandardMapper.class);
        job.setReducerClass(step4_1StandardReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
}
