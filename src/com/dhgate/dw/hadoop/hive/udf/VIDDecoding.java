package com.dhgate.dw.hadoop.hive.udf;
//import Decoder.BASE64Decoder;
import sun.misc.BASE64Decoder;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
public class VIDDecoding  extends UDF {
	 public static SimpleDateFormat dsf = new SimpleDateFormat(
		   "yyyy-MM-dd HH:mm:ss");
	 public static BASE64Decoder decode = new BASE64Decoder();
	 
	public String evaluate(String input) throws IOException
  {
	    if ((input == null) || (input.length() == 0)) {
	      return null;
    }
   //String vid = (String)input.get(0);
	    String vid = input;
   if (vid.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
	     return vid.substring(0, 10);
	    }
	     long ngxtime = createtime(vid);

	   return dsf.format(new Date(ngxtime * 1000L)).toString();
   }
	 
	 public static long createtime(String vid)
  {
    byte[] b =  new byte[0];
	    try {
	        b = decode.decodeBuffer(vid);
	
	        byte[] vidtime = new byte[4];
	        System.arraycopy(b, 4, vidtime, 0, 4);
	        long ngxtime = ngx_ntohl(vidtime);
	        return ngxtime;
	      } catch (Exception e) {
	        System.out.println(vid + ":" + e.getMessage());
	      }return 0L;
	    }
 
	 
	 private static int ngx_ntohl(byte[] x)
	    {
	     int res = 0;
	     for (int i = 0; i < 4; i++)
	      {
	       res <<= 8;
	       res |= x[i] & 0xFF;
	      }
	     return res;
	   }
	 
	 public static void main(String[] args)   throws IOException
		  {
		 String vid = "00gXxFlAs8HgH8SGpStT";
		 long ngxtime = createtime(vid);
		     System.out.println("vid time is:" + new Date(ngxtime * 1000L).toString());
		     System.out.println(dsf.format(new Date(ngxtime * 1000L)));
		 VIDDecoding aa = new VIDDecoding();
		 System.out.println(aa.evaluate(vid));
	 }
	 }
	 
