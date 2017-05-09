package com.dhgate.dw.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class FuncGetSectionIdByOrderNum extends UDF {

	public String evaluate(int pay_order_cnt)
	{
		String res = "9999";
		if (1 == pay_order_cnt){
			res = "1001";
		}else if (2 == pay_order_cnt){
			res = "1002";
		}else if (3 == pay_order_cnt){
			res = "1003";
		}else if (4 == pay_order_cnt){
			res = "1004";
		}else if (5 == pay_order_cnt){
			res = "1005";
		}else if (6 <= pay_order_cnt && 10 >= pay_order_cnt){
			res = "1006";
		}else if (11 <= pay_order_cnt && 15 >= pay_order_cnt){
			res = "1007";
		}else if (16 <= pay_order_cnt && 20 >= pay_order_cnt){
			res = "1008";
		}else if (20 <= pay_order_cnt){
			res = "1009";
		}
		
		return res;
	}
	
	/*public static void main(String[] args) {
		System.out.println(new FuncGetSectionIdByOrderNum().evaluate(10));
	}*/
}
