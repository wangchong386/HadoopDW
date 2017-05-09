package com.dhgate.dw.hadoop.hive.udf;
import org.apache.hadoop.hive.ql.exec.UDF;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
/*页面类型和日志分类对页面类型进行分类*/
public class FuncGetPageTp extends UDF { 
	public String evaluate(String param1,String param2){
		String result;
		if (("Item_U0001".equalsIgnoreCase(param1)) && (!"st".equalsIgnoreCase(param2)))
			  result = "ITEM";
			else if ("Item_U0001".equalsIgnoreCase(param1))
			  result = "STORE";
			else if (("Public_S0003".equalsIgnoreCase(param1)) && ("st".equalsIgnoreCase(param2)))
			  result = "STORE";
			else if (("Public_S0003".equalsIgnoreCase(param1)) && ("main".equalsIgnoreCase(param2)))
			  result = "MAIN";
			else if (("Public_S0003".equalsIgnoreCase(param1)) && ("lp".equalsIgnoreCase(param2)))
			  result = "LP";
			else if (("Public_S0003".equalsIgnoreCase(param1)) && ("DCP".equalsIgnoreCase(param2)))
			  result = "DCP";
			else if (("Public_S0003".equalsIgnoreCase(param1)) && ("mydh".equalsIgnoreCase(param2)))
			  result = "MYDHGATE";
			else if ("Checkout_S0001".equalsIgnoreCase(param1))
			  result = "CART";
			else if ("Checkout_S0003".equalsIgnoreCase(param1))
			  result = "PLACEORDER";
			else if ("Checkout_S0005".equalsIgnoreCase(param1))
			  result = "PAYORDER";
			else if ("Payment_U0004".equalsIgnoreCase(param1))
			  result = "PAYMENT";
			else if ("Buyer_S0001".equalsIgnoreCase(param1))
			  result = "REGISTBUYER";
			else if ("Buyer_S0002".equalsIgnoreCase(param1)) {
			  result = "LOGINBUYER";
			}
			else {
			  result = "UNKNOWN";
		
	}
		return result;
	}
	
	  public static void main(String args[])
	    {
		  FuncGetPageTp t = new FuncGetPageTp();
	        System.out.println(t.evaluate("Item_U0001", "st"));
	    }
	}

