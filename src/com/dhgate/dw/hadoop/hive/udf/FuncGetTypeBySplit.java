package com.dhgate.dw.hadoop.hive.udf;
import org.apache.hadoop.hive.ql.exec.UDF;
public class FuncGetTypeBySplit extends UDF {
	 public String evaluate(String keyValue, String bigSep, int returnType)
	    {
	     if (keyValue == null || bigSep == null || keyValue == "" || bigSep == "" )
	            return null;
	  String[] obj=keyValue.split(bigSep);
	  String a = obj[returnType];
	  return a;
	    }
	  public static void main(String args[])
	    {
		  FuncGetTypeBySplit bbb = new FuncGetTypeBySplit();
		  System.out.println(bbb.evaluate("00#00#01#2#banner1", "#", 2)); 
	    }
}
