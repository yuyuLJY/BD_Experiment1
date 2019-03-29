package Experiment1;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
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

public class Step3 {
    static double max=0;
    static double min=1000;
    
	public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
		value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
		e.printStackTrace();}
		return new Text(value);
	}
    
	public static class step3StandardMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			//value = transformTextToUTF8(value,"UTF-8");
			String[] splitResult = value.toString().split("\\|");//特殊字符
			//System.out.println("begin-----");
			if(!splitResult[6].equals("?")) {
				double rating = Double.valueOf(splitResult[6]);
				if(rating>max) {
					max = rating;
				}
				if(rating<min) {
					//System.out.println("map:"+value.toString());
					min = rating;
				}
			}
			context.write(new Text(), value);
			//context.write(new Text(), value);//<"doctor",整条信息>
		}
	}
	
	public static class step3StandardReducer extends Reducer<Text,Text,Text,Text> {
		  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  System.out.println("reducer-------------");
			  //ArrayList<String> infoList = new  ArrayList<String>();
			  double standardRate =0;
			  //mouth映射
			  Map<String,String> month = new HashMap<String,String>(); 
				month.put("01", "January");
				month.put("02", "February");
				month.put("03", "March");
				month.put("04", "April");
				month.put("05", "May");
				month.put("06", "June");
				month.put("07", "July");
				month.put("08", "August");
				month.put("09", "Septemper");
				month.put("10", "October");
				month.put("11", "November");
				month.put("12", "December");
			  //存着4 和8的数组
			  int[] dateColum = {4,8};
			  for(Text value:values) {
				  
			  String[] saveSplitResult = value.toString().split("\\|");
			  //TODO 华摄氏度数和摄氏度 组5
				if(saveSplitResult[5].contains("℃")) {
					String REGEX3 = "\\d+\\.\\d+";
					Pattern pattern3 = Pattern.compile(REGEX3);
					Matcher matcher3 = pattern3.matcher(saveSplitResult[5]);
					while(matcher3.find()) {
						double Fahrenheit = 1.8 * Double.valueOf(matcher3.group(0)) + 32;
						String newTem = String.valueOf(Fahrenheit)+"℉";
						System.out.println(newTem);
						saveSplitResult[5] = newTem;
					}	
				}
			  
			  //TODO 对日期进行处理 4 8
			  String REGEX1 = "(\\d+)-(\\d+)-(\\d+)";
			  String REGEX2 = "(\\d+)/(\\d+)/(\\d+)";
			  Pattern pattern1 = Pattern.compile(REGEX1);
				  Pattern pattern2 = Pattern.compile(REGEX2);
				  for(int i :dateColum) {
					  Matcher matcher1 = pattern1.matcher(saveSplitResult[i]);
					  if(matcher1.matches()) {
							String[] aList = saveSplitResult[i].split("-"); 
							String a1 = month.get(aList[1]);
							String newA = a1+" "+aList[2]+", "+aList[0];
							saveSplitResult[i] = newA;
							System.out.println("newA："+newA);
					  }
					  Matcher matcher2 = pattern2.matcher(saveSplitResult[i]);
					  if(matcher2.matches()) {
							String[] bList = saveSplitResult[i].split("/");
							String b1 = month.get(bList[1]);
							String newB = b1+" "+bList[2]+", "+bList[0];
							saveSplitResult[i] = newB;
							System.out.println("newB："+newB);
					  }
				  }

				  //TODO 处理rating映射范围为[0,1]
				  if(!saveSplitResult[6].equals("?")) {
					  //计算出在[0,1]范围内
					  standardRate = (Double.valueOf(saveSplitResult[6])-min)/(max-min);
					  saveSplitResult[6] = String.valueOf(standardRate);//重新定义[0,1]值
					  //重连连起来
					  String newString="";
					  for(String s :saveSplitResult) {
						  newString = newString+s+"|";
					  }
					  //infoList.add(newString);
					  System.out.println("reducer:"+newString);
					  //没有缺省值的插进去
					  context.write(new Text(newString),new Text());
				  }else {
					  //缺省值
					  context.write(new Text(value.toString()),new Text());
				  }
			  }
			  System.out.println("finishReducer");
			  //验证是否计算正确
			  //for(String s :infoList) {
				  //System.out.println("modify:"+s);
			  //}
			  System.out.printf("max:%f  min:%f\n",max,min);
			  //System.out.printf("length:%d\n",infoList.size());
		  }
	}
	
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException{
		Job job = new Job(Main.config(),"step3");
        String input = path.get("Step3Input");
        String output = path.get("Step3Output");
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setJarByClass(Step3.class);
        job.setMapperClass(step3StandardMapper.class);
        job.setReducerClass(step3StandardReducer.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job,new Path(output) );
		System.exit(job.waitForCompletion(true) ? 0 : 1);//若执行完毕，退出
	}
}
