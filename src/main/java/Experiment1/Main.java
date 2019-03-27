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
		path.put("Step1Input", HDFS+"/BigDataAnaly/experiment1/Rawdata/small_data.txt");
		path.put("Step1Output", HDFS+"/BigDataAnaly/experiment1/D_Sample/5");
		//TODO 分层抽样
		Step1.run(path);
		//TODO 
		//TODO
		//TODO
		
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
