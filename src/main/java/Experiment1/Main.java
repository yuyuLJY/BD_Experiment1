package Experiment1;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
public class Main {
	public static final String HDFS = "hdfs://192.168.126.132:9000";
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Map<String,String> path = new HashMap<String,String>();
		path.put("Step1Input", HDFS+"/BigDataAnaly/experiment1/Rawdata/large_data.txt");
		path.put("Step1Output", HDFS+"/BigDataAnaly/experiment1/D_Sample");
		//D_Sample/3是不进行encoding转换的
		
		path.put("Step2Input", HDFS+"/BigDataAnaly/experiment1/D_Sample");
		path.put("Step2Output", HDFS+"/BigDataAnaly/experiment1/D_Filtered");
		
		path.put("Step3Input", HDFS+"/BigDataAnaly/experiment1/D_Filtered");
		path.put("Step3Output", HDFS+"/BigDataAnaly/experiment1/D_Standard");
		
		path.put("Step4_1Input", HDFS+"/BigDataAnaly/experiment1/D_Filtered");
		path.put("Step4_1Output", HDFS+"/BigDataAnaly/experiment1/D_Preprocessed/Rating1/8");
		
		path.put("Step4_2Input", HDFS+"/BigDataAnaly/experiment1/D_Filtered");
		path.put("Step4_2Output", HDFS+"/BigDataAnaly/experiment1/D_Preprocessed/Rating2/6");
		//TODO 分层抽样
		//Step1.run(path);
		//TODO 过滤掉奇异值
		//Step2.run(path);
		//TODO 归一化和标准化
		//Step3.run(path);
		//TODO 找出id
		//Step4_1.run(path);
		
		//TODO 求出平均值
		Step4_2.run(path);
		
	}
	
    public static JobConf config() {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("Recommend");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
}
