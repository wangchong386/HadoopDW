package com.dhgate.dw.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class FuncGetRfxChannelDetail extends UDF {
	public String evaluate(String param1){
	String lowerParam1;
	 if(null != param1 && !param1.equals("")){
		 lowerParam1 = param1.toLowerCase();
	 }else{
		 return "None";
	 }
	 if(lowerParam1.startsWith("b2bmarketing|social|tw|") || lowerParam1.startsWith("twitter|") 
				|| lowerParam1.startsWith("t|") || lowerParam1.startsWith("social|twitter|") )
		{ //bbs
			 return "Twitter";
		}
	 
	 if(lowerParam1.startsWith("b2bmarketing|social|fb|") || lowerParam1.startsWith("facebook|")|| lowerParam1.startsWith("social|facebook|"))
		{ //bbs
			 return "Facebook";
		}
	 if(lowerParam1.startsWith("b2bmarketing|social|pn|") || lowerParam1.startsWith("social|pinterest|"))
		{ //bbs
			 return "Pinterest";
		}
	 if(lowerParam1.startsWith("b2bmarketing|social|ig|") || lowerParam1.startsWith("social|instagram|"))
		{ //bbs
			 return "Instagram";
		}
	 
	 if(lowerParam1.indexOf("referralcandy")>-1)
		{ //bbs
			 return "Referral";
		}
	 
	 if(lowerParam1.indexOf("youtube")>-1)
		{ //bbs
			 return "YouTube";
		}
	 
	 if(lowerParam1.indexOf("|affiliate|")>-1 || lowerParam1.startsWith("bm|aff|cj"))
		{ //bbs
			 return "CJ";
		}
	 
	 if(lowerParam1.indexOf("bm|aff|neverblue")>-1 || lowerParam1.startsWith("bm|aff|neverblue"))
		{ //bbs
			 return "Neverblue";
		}
	 
	 if(lowerParam1.indexOf("adwords")>-1 && lowerParam1.indexOf("paid_search")>-1)
		{ //bbs
			 return "Google search";
		}
	 
	 if(lowerParam1.indexOf("adwords")>-1 && 
			 ( lowerParam1.indexOf("contextual")>-1 ||lowerParam1.indexOf("remarketing")>-1 ||lowerParam1.indexOf("gsp")>-1 ||lowerParam1.indexOf("dyn")>-1 ))
		{ //bbs
			 return "Google Display";
		}
	 
	 if(lowerParam1.indexOf("adcenter")>-1 )
		{ //bbs
			 return "Bing";
		}
	 if(lowerParam1.indexOf("criteo")>-1 )
		{ //bbs
			 return "Criteo";
		}
	 if(lowerParam1.indexOf("gmc")>-1 )
		{ //bbs
			 return "Google shopping";
		}
	 if(lowerParam1.startsWith("txd|") ||lowerParam1.startsWith("seo|") ||lowerParam1.indexOf("organic")>-1)
		{ //bbs
			 return "SEO";
		}
	 if(lowerParam1.indexOf("edm")>-1)
		{ //bbs
			 return "Edm";
		}
	 if(lowerParam1.equals("null"))
		{ //bbs
			 return "None";
		}
	return "Others";
}
	public static void main(String args[]){
		FuncGetRfxChannelDetail test = new FuncGetRfxChannelDetail();
		System.out.println(test.evaluate("bm%7C%7C%7Cremarketing%7CCriteo%7C%7CItemcode%7CDC%7C%7C"));
		System.out.println(test.evaluate("null"));
	}
}