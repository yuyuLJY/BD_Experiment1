package Experiment1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step4_2 {
	/**
	 * 方法(1)先找出有？的式子 (2)用平均数的方法来求均值
	 */

	static double hightRate = 1.04;
	static double lowRate = 0.96;
	static ArrayList<String[]> ratingLose = new ArrayList<String[]>();

	public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new Text(value);
	}

	public static class step4_2StandardMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// value = transformTextToUTF8(value,"UTF-8");
			// TODO 找出缺失值，并且记录下其他四个值的信息，然后把符合条件的项目传进去
			String user_income_Lose;// 缺失项的相关值
			String longitude_Lose;
			String latitude_Lose;
			String altitude_Lose;
			double low_range_user_income;// 缺失项的范围
			double low_range_longitude;
			double low_range_latitude;
			double low_range_altitude;
			double high_range_user_income;// 缺失项的范围
			double high_range_longitude;
			double high_range_latitude;
			double high_range_altitude;
			String[] findMatch = value.toString().split("\\|");// 所有的项
			for (String saveSplitResult[] : ratingLose) {
				System.out.println("缺失项的内容：" + saveSplitResult[1] + " " + saveSplitResult[2] + " " + saveSplitResult[3]
						+ " " + saveSplitResult[11]);
				high_range_user_income = Float.parseFloat(saveSplitResult[11]) * hightRate;
				low_range_user_income = Float.parseFloat(saveSplitResult[11]) * lowRate;
				high_range_longitude = Float.parseFloat(saveSplitResult[1]) * hightRate;
				low_range_longitude = Float.parseFloat(saveSplitResult[1]) * lowRate;
				high_range_latitude = Float.parseFloat(saveSplitResult[2]) * hightRate;
				low_range_latitude = Float.parseFloat(saveSplitResult[2]) * lowRate;
				high_range_altitude = Float.parseFloat(saveSplitResult[3]) * hightRate;
				low_range_altitude = Float.parseFloat(saveSplitResult[3]) * lowRate;
				System.out.println("high_range_altitude:"+high_range_altitude+" low_range_altitude:"+low_range_altitude);
				if ((Float.parseFloat(findMatch[11]) < high_range_user_income)
						&& (Float.parseFloat(findMatch[11]) > low_range_user_income)) {// user_income
					context.write(new Text(saveSplitResult[11]), value);
					//System.out.println("加入user_income：" + value.toString());
				}
				if ((Float.parseFloat(findMatch[1]) < high_range_longitude)
						&& (Float.parseFloat(findMatch[1]) > low_range_longitude)) {// user_income
					context.write(new Text(saveSplitResult[0]), value);
					//System.out.println("加入longitude：" + value.toString());
				}
				if ((Float.parseFloat(findMatch[2]) < high_range_latitude)
						&& (Float.parseFloat(findMatch[2]) > low_range_latitude)) {// user_income
					context.write(new Text(saveSplitResult[0]), value);
					//System.out.println("加入latitude：" + value.toString());
				}
				if ((Float.parseFloat(findMatch[3]) < high_range_altitude)
						&& (Float.parseFloat(findMatch[3]) > low_range_altitude)) {// altitude
					context.write(new Text(saveSplitResult[0]), value);
					//System.out.println("加入altitude：" + value.toString());
				}
			}
		}
	}

	public static class step4_2StandardReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("reducer-------------");
			// ArrayList<String> infoList = new ArrayList<String>();
			double count = 0;
			double sum = 0;
			double ave = 0;

			for (Text value : values) {
				System.out.println(value.toString());
				String[] saveSplitResult = value.toString().split("\\|"); // 把rating的值求平均值 sum = sum+
				Float.parseFloat(saveSplitResult[6]);
				count++;
			}
			ave = sum / count;
			System.out.println(" ID:" + key + " 平均值：" + ave);
			context.write(new Text(key), new Text(String.valueOf(ave)));

		}
	}

	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = Main.config();
		conf.set("fs.defaultFS", "hdfs://192.168.126.132:9000");
		FileSystem fs = FileSystem.get(conf);

		// TODO 读出信息源的数量，执行十遍，！！！覆盖问题
		Path pathSourceNumber = new Path(
				"hdfs://192.168.126.132:9000/BigDataAnaly/experiment1/D_Preprocessed/Rating1/8/part-r-00000");
		if (fs.exists(pathSourceNumber)) {
			System.out.println("Exists!");
			try {
				// 此为hadoop读取数据类型
				FSDataInputStream is = fs.open(pathSourceNumber);
				InputStreamReader inputStreamReader = new InputStreamReader(is, "utf-8");
				String line = null;
				// 把数据读入到缓冲区中
				BufferedReader reader = new BufferedReader(inputStreamReader);
				// 从缓冲区中读取数据
				while ((line = reader.readLine()) != null) {
					String[] split = line.split("\\|");
					ratingLose.add(split);
					// System.out.println("line="+line);

				}
			} catch (Exception e) {
				System.out.println(e);
			}
		} else {
			System.out.println("不存在");
		}

		Job job = new Job(Main.config(), "step4_2");
		String input = path.get("Step4_2Input");
		String output = path.get("Step4_2Output");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(Step4_2.class);
		job.setMapperClass(step4_2StandardMapper.class);
		job.setReducerClass(step4_2StandardReducer.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);// 若执行完毕，退出
	}
}
