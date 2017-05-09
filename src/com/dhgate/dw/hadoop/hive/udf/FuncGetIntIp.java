package com.dhgate.dw.hadoop.hive.udf;
import java.io.IOException;



//import com.dhgate.dw.hadoop.*;
import org.apache.hadoop.hive.ql.exec.UDF;
public class FuncGetIntIp extends UDF {
	
	public String evaluate(String longIp)  throws IOException
	{
		//long longIp1 = Long.valueOf(longIp).longValue();
		//longIp1 = Long.toString(longIp);
		//Long ipL = new Long((String)input.get(0));
		//return IPUtil.longToIP(ipL.longValue());
		Long a;
		try {
		  a =  new Long(longIp);
		}catch(Exception e) {
			return "";
		}
		 StringBuffer sb = new StringBuffer("");
		 sb.append(String.valueOf((a >>> 24)));
         sb.append(".");
         sb.append(String.valueOf((a & 0x00FFFFFF) >>> 16));
         sb.append(".");
         sb.append(String.valueOf((a & 0x0000FFFF) >>> 8));
         sb.append(".");
         sb.append(String.valueOf((a & 0x000000FF)));
         return sb.toString();
          //Long ipL = new Long((String)longIp.get(0));
           // return IPUtil.longToIP(ipL.longValue());
         //return IPUtil.longToIP(longIp.longValue());
		 //return "null";
         
         //     Long ipL = new Long((String)input.get(0));
         //     return IPUtil.longToIP(ipL.longValue());
	}
	public static void main(String[] args) throws IOException{
		//String ipStr = "58.50.24.78";
        //long longIp = evaluate.ipToLong(ipStr);
        //System.out.println("整数形式为：" + longIp);
		FuncGetIntIp a =new FuncGetIntIp();
		
        System.out.println(a.evaluate("3359022626"));
        //ip地址转化成二进制形式输出
        //System.out.println("二进制形式为：" + Long.toBinaryString(longIp));
	}

}
