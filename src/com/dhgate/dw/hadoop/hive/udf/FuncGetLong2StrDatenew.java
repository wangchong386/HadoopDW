package com.dhgate.dw.hadoop.hive.udf;
import java.io.IOException;
import java.text.*;
import org.apache.hadoop.hive.ql.exec.UDF;


public class FuncGetLong2StrDatenew extends UDF {
	
	public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public String evaluate(String input,String input2)  throws IOException
	 {
	    if ((input == null) || (input.length() == 0) ) {
		     return null;
	       }

	    String sdate = "";   /*2005-01-01 00:00:01*/
	    Long ldate =0L;
  // Long ldate = new Long(input.getBytes().toString());
	       try{
		       ldate = new Long(input.toString());
		       sdate = sdf.format(ldate);
	       }catch(Exception io){
	    	   //System.out.println("error");
	    	   sdate = input2 + " 00:00:00";
	       }
	       return sdate;
	   }
	
	
	
  public static void main(String[] args) throws IOException{
	  FuncGetLong2StrDatenew p =new FuncGetLong2StrDatenew();
	  String str = "1421510394774";
	  System.out.println(p.evaluate(str,"2015-01-02"));
	  //System.out.println(p.evaluateInt(str));
	  //System.out.println(NumberUtils.isDigits("1a"));
			   }
  
  
 
}
