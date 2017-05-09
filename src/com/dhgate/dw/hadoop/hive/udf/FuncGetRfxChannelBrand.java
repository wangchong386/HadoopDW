package com.dhgate.dw.hadoop.hive.udf;
import org.apache.hadoop.hive.ql.exec.UDF;
public class FuncGetRfxChannelBrand extends UDF {
	
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
		 if(lowerParam1.indexOf("adwords")>-1 &&  lowerParam1.indexOf("paid_search")>-1 &&  lowerParam1.indexOf("dhgate")>-1 )
			{ //bbs
				 return "GS-brand";
			}
		 if(lowerParam1.indexOf("adwords")>-1 &&  lowerParam1.indexOf("paid_search")>-1 )
			{ //bbs
				 return "GS-非brand";
			}
		 
		 if(lowerParam1.indexOf("adcenter")>-1 &&  lowerParam1.indexOf("dhgate")>-1 )
			{ //bbs
				 return "Bing-brand";
			}
		 
		 if(lowerParam1.indexOf("adcenter")>-1 &&  lowerParam1.indexOf("dhgate")>-1 )
			{ //bbs
				 return "Bing-brand";
			}
		return "Others";
	}
	public static void main(String args[]){
		FuncGetRfxChannelBrand test = new FuncGetRfxChannelBrand();
		System.out.println(test.evaluate("adwordspaid_search"));
		System.out.println(test.evaluate("adwords"));
	}

}
