package Experiment1;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
	public static void main(String[] args) {
		//2018-03-21、2018/03/21、March 21, 2019
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
		String a = "2018-06-21";
		String b = "2018/06/21";
		String c = "March 21, 2019";
		//全都变成2018-03-21
		String REGEX1 = "(\\d+)-(\\d+)-(\\d+)";
		String REGEX2 = "(\\d+)/(\\d+)/(\\d+)";
		Pattern pattern1 = Pattern.compile(REGEX1);
		Matcher matcher1 = pattern1.matcher(a);
		if(matcher1.matches()) {
			String[] aList = a.split("-"); 
			String a1 = month.get(aList[1]);
			String newA = a1+" "+aList[2]+", "+aList[0];
			System.out.println("newA："+newA);
		}
		Pattern pattern2 = Pattern.compile(REGEX2);
		Matcher matcher2 = pattern2.matcher(b);
		if(matcher2.matches()) {
			String[] bList = b.split("/");
			String b1 = month.get(bList[1]);
			String newB = b1+" "+bList[2]+", "+bList[0];
			System.out.println("newB："+newB);
		}
		System.out.println("a: "+matcher1.matches());
		System.out.println("b: "+matcher2.matches());
	}
}
