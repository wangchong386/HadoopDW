package com.dhgate.dw.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class FuncGetSectionIdByOrderAmount extends UDF{
	
	public String evaluate(double pay_order_amount)
	{
		String res = "9999";
		if (0 <= pay_order_amount && 20 >= pay_order_amount){
			res = "2001";
		}else if (21 <= pay_order_amount && 50 >= pay_order_amount){
			res = "2002";
		}else if (51 <= pay_order_amount && 100 >= pay_order_amount){
			res = "2003";
		}else if (101 <= pay_order_amount && 150 >= pay_order_amount){
			res = "2004";
		}else if (151 <= pay_order_amount && 200 >= pay_order_amount){
			res = "2005";
		}else if (201 <= pay_order_amount && 300 >= pay_order_amount){
			res = "2006";
		}else if (301 <= pay_order_amount && 500 >= pay_order_amount){
			res = "2007";
		}else if (501 <= pay_order_amount && 1000 >= pay_order_amount){
			res = "2008";
		}else if (1001 <= pay_order_amount && 2000 >= pay_order_amount){
			res = "2009";
		}else if (2001 <= pay_order_amount && 4000 >= pay_order_amount){
			res = "2010";
		}else if (4001 <= pay_order_amount && 10000 >= pay_order_amount){
			res = "2011";
		}else if (10000 < pay_order_amount){
			res = "2012";
		}
		return res;
	}
	
	/*public static void main(String[] args) {
		System.out.println(new FuncGetSectionIdByOrderAmount().evaluate(10001));
	}*/
}
