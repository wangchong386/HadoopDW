package com.dhgate.dw.hadoop.hive.udf;

import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

//import com.sun.org.apache.xerces.internal.impl.xs.identity.Selector.Matcher;
//import org.w3c.dom.Text;
//import org.apache.hadoop.io.Text;

//import java.util.regex.Pattern;
//页面分类处理逻辑//
public class FuncGetTypeCode  extends UDF {
	 public String evaluate(String search_type,String keywords,String expose_type,String expo_order
			,String expose_position,String ref_urL){
		
// search_type :搜索来源类别  
//  keywords :搜索关键字
//  expose_type :产品类型 
// expo_order :曝光顺序
// expose_position:曝光位置序号 
// ref_urL ：引用URL
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
		String ref_urL_lw = ref_urL.toString().toLowerCase();
		//System.out.println(expose_type.toLowerCase());
		String temp_c[] = expose_type.toLowerCase().split("\\|");//c
		String temp_s = search_type.toLowerCase();//S
		String temp_a = expose_position.toLowerCase();//a
	    System.out.println(ref_urL_lw);
		//Pattern pattern = Pattern.compile("[0-9][a-z]");
		//s23456
		Pattern pattern = Pattern.compile("[0-9]"); 
		Pattern pattern1 = Pattern.compile("[a-z]");
		Pattern pattern2 = Pattern.compile("^(http[Ss]*:\\/\\/|www)[a-z]{0,20}.?dhgate.com");
		//String m=pattern2.matcher(ref_urL_lw).toString();
		//System.out.println(pattern1);
		//Pattern pattern2 = Pattern.compile("httdhgate.com?");
		//System.out.println(ref_urL_lw.indexOf("dhgate"));
		
		//System.out.println(temp_c.substring(1,2));
		
		//站内外标志
		if (ref_urL_lw == null || ref_urL_lw.equals(""))
		{
			
			site_in_out = "2";
		}
		//http://www.dhgate.com/activities
		else if (pattern2.matcher(ref_urL_lw).find())
		{
			site_in_out = "0";
		}
		
		else 
		{
			site_in_out = "1";
		}	
		
		
       if (temp_s.equals("ks") ) 
			source_type = "21";
       else if ((temp_s.equals("as")) && (keywords != ""))
    	  source_type = "21";
       else if ((temp_s.equals("as")) && (keywords == ""))
    	  source_type = "20";
	   else if (temp_s.equals("cs")) 
		  source_type = "20";
       
       
	    if ((temp_a.startsWith("hp")) ||( temp_a.startsWith("hu"))) 
	  {
		  source_type = "00";
		  expo_module = "00";
	      expo_module_dtl = expo_order;
		  expo_type = "99"; 
		  }
		//以s开头，且第二个字符为数字或者字符
	   else  if  ((temp_a.startsWith("s")) && (
			   pattern.matcher(temp_a.substring(1,2)).matches() || 
			   pattern1.matcher(temp_a.substring(1,2)).matches())	
			   )
	  {
		  expo_module = "10";
		  expo_module_dtl ="";////////////////////////////
		  if(temp_c.length>0){
		     expo_type = temp_c[0];
		  }
		  
	  }
	  else if  (temp_a.equals("gw"))
	  {
		  expo_module = "20";
		  if(temp_c.length>0){
		     expo_type = temp_c[0];
		  }
	  }
	  else if  (temp_a.equals("gp"))
	  {
		  expo_module = "21";
		  if(temp_c.length>0){
		     expo_type = temp_c[0];
		  }
	  }
	  else if  (temp_a.startsWith("sshot"))
	  {   
		  source_type ="10";
		  expo_module = "50";//20150316根据模型修改
		  expo_type = "99";
	  }
	  else if  (temp_a.startsWith("ssreview"))
	  {   
		  source_type ="10";
		  expo_module = "51";
		  expo_type = "99";
	  }
	 
	  else if  (temp_a.startsWith("svh"))
	  {   
		  source_type ="10";
		  expo_module = "52";
		  expo_type = "99";
	  }
	 //以cp开头但排除cpmy cpmf
	  else if  ((temp_a.startsWith("cp")) && (temp_a.indexOf("cpmy")<0) && (temp_a.indexOf("cpmf")<0))
	  {   
		  
		 
		  expo_module ="30";
		  
		  if(temp_c.length>0){
		     expo_type = temp_c[0];
		  }
		  
		  if (ref_urL_lw.indexOf(".dhgate.com/product")>-1)
		  {
			  source_type = "30";
		  }
		  else if (ref_urL_lw.indexOf(".dhgate.com/store/product")>-1)
		  {
			  source_type = "10";
		  }
		  else 
			  source_type = "99";
		  		  
	  }
	 
	  else if  (temp_a.equals("cpmy") || temp_a.equals("cpmf") || temp_a.startsWith("mydh"))
	  {
		  source_type ="70";
		  
		  expo_module = "30";
		  if(temp_c.length>0){
		     expo_type = temp_c[0];
		  }
		  
	  }
	 
	  else if  ((temp_a.startsWith("dcp")))
			  {
		      
		  source_type ="80";
		  expo_module = "99";
		  expo_type = "99";
			  }
	 
	 
	  else if  ((temp_a.startsWith("listing")))
	 {
		      
		  source_type ="99";
		  expo_module = "60";
		  if(temp_c.length>0){
		    expo_type = temp_c[0];
		  }
	  }
	
//	    String lowerParam1;
//		 if(null != param1 && !param1.equals("")){
//			 lowerParam1 = param1.toLowerCase();
//		 }else{
//			 return "Null";
//		 }
	    
    if (source_type == "" || source_type == null || source_type.equals(""))
    { 
			source_type = "99";
		
	}
    
    if (expo_module == "" || expo_module == null || expo_module.equals(""))
    { 
	   expo_module = "99";
	
     }

    if (expo_type == "" || expo_type == null|| expo_type.equals(""))
    { 
	   expo_type = "99";
	
    }
    
    if (expo_module_dtl == "" ||expo_module_dtl == null|| expo_module_dtl.equals(""))
    { 
	   expo_module_dtl = "99";
	
    }

    return source_type + "#" + expo_module + "#"  
       + expo_type + "#" + site_in_out + "#" + expo_module_dtl;
    

	}
	

	

	 public static void main(String args[])
	 {
		// String test1 = "111";
		 FuncGetTypeCode aa = new FuncGetTypeCode();
		// search_type :搜索来源类别     keywords :搜索关键字   
		// expose_type :产品类型   expo_order :曝光顺序    expose_position:曝光位置序号
		 System.out.println(aa.evaluate("ks","",
				 "6|ff8080814ac74328014ae160ca765b28:00000000000000000000000000000000",
				 "banner1","hplisting","http://dhgate.com/ho"));
		 //System.out.println(test1);
	 }
}
