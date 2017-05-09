package com.dhgate.dw.hadoop.hive.udf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

public class FuncGetDate extends UDF {
	
	/**
	 * 
	 * @param param1
	 * @param param2
	 * @return
	 *   ="bbs"       .equals("bbs")           //等于bbs
	 *   "hd%"        .startsWith("hd")        //hd开头
	 *   "%huodong%"  .indexOf("huodong")>-1   //中间含有huodong
	 *   "%edm"       .endsWith("edm")         //以edm结尾
	 *   and           &&                      //与的关系
	 *   or            ||                      //或的关系
	 * @throws ParseException 
	 */
	
	
	public int evaluate(String param1) throws ParseException {
		/**  
		  * 根据日期字符串判断当月第几周  
		  * @param str  
		  * @return  
		  * @throws Exception  
		  */  

		     SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd");  
		     Date date =sdf.parse(param1);  
		     Calendar calendar = Calendar.getInstance();  
		     calendar.setTime(date);  
		     //第几周  
		     int week = calendar.get(Calendar.WEEK_OF_MONTH);  
		     //第几天，从周日开始  
		     int day = calendar.get(Calendar.DAY_OF_WEEK);  
		     return week;  
		 } 

	public static void main(String args[]) throws ParseException {
		FuncGetDate test = new FuncGetDate();
		System.out.println(test.evaluate("2014-01-01"));
		
	}
}