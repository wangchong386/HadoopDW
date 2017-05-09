package com.dhgate.dw.hadoop.hive.udf;
import java.io.IOException;
//import java.text.*;



import org.apache.hadoop.hive.ql.exec.UDF;
public class FuncGetCountry extends UDF  {
	
	public String evaluate(String input)  throws IOException
	 {
		if ((input == null) || (input.length() == 0) ) 
		{
		     return "--";
	    }
		String str = "'AD','AE','AF','AG','AI','AL','AM',"
				+ "'AN','AO','AR','AS','AT','AU','AW','AZ','BA',"
				+ "'BB','BD','BE','BF','BG','BH','BI','BJ','BM','BN',"
				+ "'BO','BR','BS','BT','BW','BY','BZ','CA','CD','CF','CG','CH',"
				+ "'CI','CK','CL','CM','CN','CO','CR','CU','CV','CY','CZ','DE','DJ',"
				+ "'DK','DM','DO','DZ','EC','EE','EG','ER','ES','GF','GH','GI','GL','GM',"
				+ "'GN','GP','GQ','GR','GT','GU','GW','GY','HK','HN','HR','HT','HU','ID',"
				+ "'IE','IL','IN','IQ','IR','IS','IT','JM','JO','JP','KE','KG','KH','KI',"
				+ "'KM','KN','KR','KW','KY','KZ','LA','LB','LC','LI','LK','LR','LS','LT',"
				+ "'LU','LV','LY','MA','MC','MD','ME','MG','MH','MK','ML','MM','MN','MO',"
				+ "'MP','MQ','MR','MS','MT','MU','MV','MW','MX','MY','MZ','NA','NC','NE','NF',"
				+ "'NG','NI','NL','NO','NP','NR','NU','NZ','OM','PA','PE','PF','PG','PH','PK',"
				+ "'PL','PR','PT','PW','PY','QA','RE','RO','RS','RU','RW','SA','SB','SC','SD','SE'"
				+ ",'SG','SI','SK','SL','SM','SN','SO','SR','ST','SV','SY','SZ','TC','TD','TG','TH',"
				+ "'TJ','TL','TM','TN','TO','TR','TT','TV','TW','TZ','UA','UG','UK','US','UY','UZ',"
				+ "'VA','VC','VE','VG','VI','VN','VU','WF','WS','YE','ZA','ZM','ZW','ET','FI','FJ',"
				+ "'FK','FM','FO','FR','GA','GD','GE'";
		 
		//if(input.toString().contains(str.toString()))
		if(str.contains(input))
		//if(str.toString().contains(input.toString())) 
		 {
			 return input;
		 }
		 else
		return "--";
        }



	 public static void main(String[] args) throws IOException {
		 FuncGetCountry p = new FuncGetCountry();
		 //p.getcountry("222");
		 System.out.println(p.evaluate("AI"));
		// 22 System.out.println(p.talk()) ;
				   }
	
}
