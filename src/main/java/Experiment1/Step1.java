package Experiment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class Step1 {
	public static class step1StandardMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String[] splitResult = value.toString().split("\\|");//特殊字符
			System.out.printf(Arrays.toString(splitResult));
			context.write(new Text(splitResult[10]), value);//<"doctor",整条信息>
		}
	}
	
	public static class step1StandardReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			ArrayList<String> infoList = new  ArrayList<String>();
			System.out.printf("key:%s\n",key.toString());
			for(Text value:values) {
				infoList.add(value.toString());
			}
			//进行抽样
			int personSum = infoList.size();
			int sampleSum = (int) Math.floor(personSum*0.1);//ceil不小于他的最小整数
			System.out.printf("sampleSum:%d\n",sampleSum);
			int segments = personSum/sampleSum;
			//按照系统抽样的方法，抽取指定的人数
			for(int i = 0;i<infoList.size();i = i+segments) {
				context.write(new Text(infoList.get(i)),new Text());
			}
			
			//某一职业抽样完毕，进行清空列表
			infoList.clear();
		}
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(Main.config(),"step1");
        String input = path.get("Step1Input");
        String output = path.get("Step1Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(Step1.class);
        job.setMapperClass(step1StandardMapper.class);
        job.setReducerClass(step1StandardReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
}
