package com.dhgate.dw.hadoop.hive.udf;
import java.util.Date;
import java.text.SimpleDateFormat;

public class DateFormat {

     public static void main(String[] args) {
 // TODO Auto-generated method stub
 
 SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
 SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
 
 Date date = new Date();
 System.out.println(date);
 String nowTime1 = sdf1.format(date);
 String nowTime2 = "";
 try{
     nowTime2 = sdf2.format(sdf1.parse(nowTime1));  
 }catch(Exception e){
     e.printStackTrace();
 }
 
 //String nowTime2 = sdf2.format(nowTime1);
 
 System.out.println(nowTime1);
 System.out.println(nowTime2);

    }

}