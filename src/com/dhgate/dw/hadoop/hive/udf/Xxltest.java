package com.dhgate.dw.hadoop.hive.udf;

import java.security.Timestamp;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Xxltest {
	public static void main(String args[])
	{
	//int num =1;
//	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//	String time = df.format());
//	//Timestamp ts = Timestamp.valueOf(time); 
//	 //num=star(7); // 输入 7 给 star()，并以 num 接收返回的数值
//	System.out.println(time);
//	
	String sdate;
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  	 Calendar ca = Calendar.getInstance();
    // ca.set(Calendar,Date.valueOf("2015-01-01").getDate()); 
     sdate = df.format(ca.getTime()); //+ " 00:00:00";
     System.out.println(sdate);
	 }
	
	 

}
