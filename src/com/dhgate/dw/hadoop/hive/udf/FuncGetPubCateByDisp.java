package com.dhgate.dw.hadoop.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;

public class FuncGetPubCateByDisp extends UDF  {
	
	public String evaluate(String input)  throws IOException
	 {
		String lowerParam1;
		if(null != input && !input.equals("")){
			 lowerParam1 = input.substring(0, 3);
		 }else{
			 return "None";
		 }
		 if(lowerParam1.equals("105"))
			{ //bbs
				 return "135";
			}
		 if(lowerParam1.equals("018"))
			{ //bbs
				 return "018";
			}
		 if(lowerParam1.equals("104"))
			{ //bbs
				 return "009";
			}
		
		 if(lowerParam1.equals("002"))
			{ //bbs
				 return "002";
			}
		 if(lowerParam1.equals("111"))
			{ //bbs
				 return "142";
			}
		
		 if(lowerParam1.equals("103"))
			{ //bbs
				 return "008";
			}
		
		 if(lowerParam1.equals("112"))
			{ //bbs
				 return "143";
			}
		 if(lowerParam1.equals("014"))
			{ //bbs
				 return "014";
			}
		 if(lowerParam1.equals("100"))
			{ //bbs
				 return "004";
			}
		 if(lowerParam1.equals("024"))
			{ //bbs
				 return "024";
			}
		
		 if(lowerParam1.equals("019"))
			{ //bbs
				 return "019";
			}
		 if(lowerParam1.equals("102"))
			{ //bbs
				 return "006";
			}
		 if(lowerParam1.equals("106"))
			{ //bbs
				 return "136";
			}
		 if(lowerParam1.equals("007"))
			{ //bbs
				 return "007";
			}
		 if(lowerParam1.equals("113"))
			{ //bbs
				 return "144";
			}
		 if(lowerParam1.equals("110"))
			{ //bbs
				 return "141";
			}
		 if(lowerParam1.equals("109"))
			{ //bbs
				 return "140";
			}
		 if(lowerParam1.equals("107"))
			{ //bbs
				 return "137";
			}
		 
		 if(lowerParam1.equals("101"))
			{ //bbs
				 return "005";
			}
		 
		 if(lowerParam1.equals("011"))
			{ //bbs
				 return "011";
			}
		
		 if(lowerParam1.equals("108"))
			{ //bbs
				 return "139";
			}
		 
		 if(lowerParam1.equals("117"))
			{ //bbs
				 return "152";
			}
		 
		 if(lowerParam1.equals("099"))
			{ //bbs
				 return "099";
			}
		 
		 if(lowerParam1.equals("130"))
			{ //bbs
				 return "169";
			}
		  
		 return   "Others";
	 }
	
	public static void main(String[] args) throws IOException{
		FuncGetPubCateByDisp aaa = new FuncGetPubCateByDisp();
		System.out.println(aaa.evaluate(""));
	}

}
