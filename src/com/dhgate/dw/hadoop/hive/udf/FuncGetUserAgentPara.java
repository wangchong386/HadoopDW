package com.dhgate.dw.hadoop.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;

import eu.bitwalker.useragentutils.UserAgent;

public class FuncGetUserAgentPara extends UDF{

	public String evaluate(String key, String input)  throws IOException{
		String res = null;
		if (null == key || key.trim().length()<=0){
			return res;
		}
		if (null == input || input.trim().length()<=0){
			return res;
		}
		try {
			UserAgent agent = new UserAgent(input);
			if ("browser".equalsIgnoreCase(key)){
				res = agent.getBrowser().getName(); 
			}else if ("browserVersion".equalsIgnoreCase(key)){
				res = agent.getBrowserVersion().getVersion();
			}else if ("operatingSystem".equalsIgnoreCase(key)){
				res = agent.getOperatingSystem().getName();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}
	
	/*public static void main(String[] args) throws IOException {
		String browser = new FuncGetUserAgentPara().evaluate("browser", "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36");
		System.out.println(browser);
		String browserVersion = new FuncGetUserAgentPara().evaluate("browserVersion", "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36");
		System.out.println(browserVersion);
		String operatingSystem = new FuncGetUserAgentPara().evaluate("operatingSystem", "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36");
		System.out.println(operatingSystem);
	}*/

}
