package com.dhgate.dw.hadoop.hive.udf;
import org.apache.hadoop.hive.ql.exec.UDF;


public class Fn_getwisdomallcountry extends UDF {
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
	 */
	public String evaluate(String param1){
		
		String lowerParam1;
		 if(null != param1 && !param1.equals("")){
			 lowerParam1 = param1.toLowerCase();
		 }else{
			 return "Null";
		 }
	
	if(lowerParam1.startsWith("b2bmarketing|social|") || lowerParam1.startsWith("twitter|") 
			|| lowerParam1.startsWith("t|") ||lowerParam1.startsWith("facebook|") )
	{ //bbs
		 return "social";
	}
	
	if(lowerParam1.indexOf("referralcandy")>-1)
	{ //bbs
		 return "referral";
	}

	if(lowerParam1.indexOf("youtube")>-1)
	{ //bbs
		 return "YouTube";
	}
	
	if(lowerParam1.indexOf("|affiliate|")>-1 || lowerParam1.startsWith("bm|aff|") )
	{ //bbs
		 return "Affiliate";
	}
	
	if(lowerParam1.startsWith("bm|")|| lowerParam1.indexOf("adwords")>-1 || lowerParam1.indexOf("paid_search")>-1 || lowerParam1.indexOf("contextual")>-1)
	{ //bbs
		 return "PPC";
	}
	
	if(lowerParam1.startsWith("txd|")|| lowerParam1.startsWith("seo|") || lowerParam1.indexOf("organic")>-1 )
	{ //bbs
		 return "SEO";
	}
	
	if(lowerParam1.indexOf("edm")>-1 )
	{ //bbs
		 return "edm";
	}
	
	return "Others";
	
	}
	
	public static void main(String args[]){
		Fn_getwisdomallcountry test = new Fn_getwisdomallcountry();
		System.out.println(test.evaluate("b2bmarketing|social|"));
		System.out.println(test.evaluate("adwords"));
	}
	
 
}
