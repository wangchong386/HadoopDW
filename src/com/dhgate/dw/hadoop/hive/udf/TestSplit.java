package com.dhgate.dw.hadoop.hive.udf;

import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

//import java.util.regex.Pattern;

public class TestSplit  extends UDF {
	public String SplitTypeCode (String search_type,String keywords,String expose_type,String expo_order
			,String expose_position,String ref_urL){
		
// search_type :搜索来源类别     keywords :搜索关键字   expose_type :产品类型   expo_order :曝光顺序    expose_position:曝光位置序号 ref_urL ：引用URL
/*页面类型*/
//		平台首页        00
//		类目引导页DCP  01 
//		店铺            10
//		类目搜索        20
//		关键词搜索    21
//		高级搜索      22
//		商品详情页    30 
//		收藏夹页面    40
//		购物车页面    50
//		促销活动页面  60
//		营销活动页面  61
//		我的敦煌页    70
//		其他          99
		String source_type = "";//页面类型
		String expo_module = "";//曝光模块
		String expo_module_dtl = "";//曝光细模块
		String expo_type ="";//曝光展示类型
		String site_in_out = "";//站内外跳转标示
		String ref_urL_lw = ref_urL.toLowerCase();
		//System.out.println(expose_type.toLowerCase());
		String temp_c[] = expose_type.toLowerCase().split("\\|");//c
		String temp_s = search_type.toLowerCase();//S
		String temp_a = expose_position.toLowerCase();//a
		//System.out.println(temp_s);
		//Pattern pattern = Pattern.compile("[0-9][a-z]");
		//s23456
		Pattern pattern = Pattern.compile("[0-9]"); 
		Pattern pattern1 = Pattern.compile("[a-z]");
		//System.out.println(temp_c[0]);
		//System.out.println(temp_c.substring(1,2));
		if (ref_urL_lw == null && ref_urL_lw.equals(""))
		{
			
			site_in_out = "2";
		}
		
		else if (null != ref_urL_lw && !ref_urL_lw.equals("") && ref_urL_lw.indexOf(".dhgate.com\\/")>-1)
		{
			site_in_out = "1";
		}
		
		else if (null != ref_urL_lw && !ref_urL_lw.equals("") )
		{
			site_in_out = "0";
		}
       else if (temp_s.equals("ks") ) 
			source_type = "21";
       else if ((temp_s.equals("as")) && (keywords != ""))
    	  source_type = "21";
       else if ((temp_s.equals("as")) && (keywords == ""))
    	  source_type = "20";
	   else if (temp_s.equals("cs")) 
		  source_type = "20";
	   else if ((temp_c[0].startsWith("hp")) ||( temp_c[0].startsWith("hu"))) 
	  {
		  source_type = "00";
		  expo_module = "00";
	      expo_module_dtl = expo_order;
		  expo_type = "01"; 
		  }
		//以s开头，且第二个字符为数字或者字符
	   else  if  ((temp_c[0].startsWith("s")) && (pattern.matcher(temp_c[0].substring(1,2)).matches() || pattern1.matcher(temp_c[0].substring(1,2)).matches())
			)
	  {
		  expo_module = "10";
		  expo_module_dtl ="";////////////////////////////
		  expo_type = temp_a;
		  
	  }
	  else if  (temp_c.equals("gw"))
	  {
		  expo_module = "20";
		  expo_type = temp_a;
	  }
	  else if  (temp_c.equals("gp"))
	  {
		  expo_module = "30";
		  expo_type = temp_a;
	  }
	  else if  (temp_c[0].startsWith("sshot"))
	  {   
		  source_type ="10";
		  expo_module = "60";
		  expo_type = "01";
	  }
	  else if  (temp_c[0].startsWith("ssreview"))
	  {   
		  source_type ="10";
		  expo_module = "61";
		  expo_type = "01";
	  }
	 
	  else if  (temp_c[0].startsWith("svh"))
	  {   
		  source_type ="10";
		  expo_module = "50";
		  expo_type = "01";
	  }
	 //以cp开头但排除cpmy cpmf
	  else if  ((temp_c[0].startsWith("cp")) && (temp_c[0].indexOf("cpmy")<0) && (temp_c[0].indexOf("cpmf")<0))
	  {   
		  
		 
		  expo_module ="40";
		  expo_type = temp_a;
		  if (ref_urL_lw.indexOf(".dhgate.com\\/product")>-1)
		  {
			  source_type = "30";
		  }
		  else if (ref_urL_lw.indexOf(".dhgate.com\\/store\\/product")>-1)
		  {
			  source_type = "10";
		  }
		  else 
			  source_type = "99";
		  		  
	  }
	 
	  else if  (temp_c[0].equals("cpmy") || temp_c[0].equals("cpmf") || temp_c[0].startsWith("mydh"))
	  {
		  source_type ="70";
		  
		  expo_module = "40";
		  expo_type =temp_a;
		  
	  }
	 
	  else if  ((temp_c[0].startsWith("dcp")))
			  {
		      
		  source_type ="01";
		  expo_module = "99";
		  expo_type = "01";
			  }
	 
	 
	  else if  ((temp_c[0].startsWith("listing")))
	 {
		      
		  source_type ="99";
		  expo_module = "66";
		  expo_type = temp_a;
	  }
		
if (source_type == "")
    { 
			source_type = "99";
		
	}
if (expo_module == "")
{ 
	expo_module = "99";
	
}

if (expo_type == "")
{ 
	expo_type = "99";
	
}
if (expo_module_dtl == "")
{ 
	expo_module_dtl = "99";
	
}
return source_type + "#" + expo_module + "#"  + expo_type + "#" + site_in_out + "#" + expo_module_dtl;
	}
	

		//System.out.println(array[3]);
	

	 public static void main(String args[])
	    {
		// String test1 = "111";
		 TestSplit a = new TestSplit();
		// search_type :搜索来源类别     keywords :搜索关键字   expose_type :产品类型   expo_order :曝光顺序    expose_position:曝光位置序号
		 System.out.println(a.SplitTypeCode("s23232","222","s3456|FGFFG","22","222","http://www.dhgate.com/store/product/sexy-wedding-dresses-2015-new-design-unique/217369219.html"));
		 //System.out.println(test1);
	    }
}
