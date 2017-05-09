package com.dhgate.dw.hadoop.hive.udf;

//import java.util.Arrays;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.hive.ql.exec.UDF;

//import com.sun.org.apache.xerces.internal.impl.xs.identity.Selector.Matcher;

public class FuncGetTypeCodeBrowser  extends UDF {
	 public String evaluate(String ref_url,String  click_position,String prod_click_type,String expo_order){
		
		 
		 //ref_url 引用url
		//click_position 浏览位置  对应#A-B-C|D的A部分,如s*
		//点击浏览类型prod_click_type    对应#A-B-C|D的C|D部分,如2|4554121
		//产品曝光排名expo_order
		String source_type = "";//浏览来源类型
		String browser_module = "";//浏览模块
		String browser_type = "";//浏览展示类型
		String browser_dtl_module ="";//浏览明细模块
		String site_in_out = "";//站内外跳转标示
		String browser_page = "";//浏览列表页码
		String  ext_id ="";//扩展id
		int aaa = 0;
		
		String ref_urL_lw = ref_url.toLowerCase();
		//String current_url_lw = current_url.toLowerCase();
		String click_position_lw = click_position.toLowerCase();//a
		String prod_click_type_lw[] = prod_click_type.toLowerCase().split("\\|");//c
		String ext_id1[] =click_position_lw.split("_");
	    //System.out.println(click_position_lw);
		//Pattern pattern = Pattern.compile("[0-9][a-z]");
		//s23456
		Pattern pattern = Pattern.compile("[0-9]"); 
		Pattern pattern1 = Pattern.compile("[a,b,c,d,e,s]");
		Pattern pattern2 = Pattern.compile("^(http[Ss]*:\\/\\/|www)[a-z]{0,20}.?dhgate.com");
		//String m=pattern2.matcher(ref_urL_lw).toString();
		//System.out.println(pattern1);
		//Pattern pattern2 = Pattern.compile("httdhgate.com?");
		//System.out.println(ref_urL_lw.indexOf("dhgate"));
		
		Matcher matcher1 = pattern.matcher(click_position_lw);
		if (matcher1.find()){
			aaa =click_position_lw.indexOf(matcher1.group(0));
		}
		
		
		String as[] = new String[10];
		//标记字符A后面的数字
		if (pattern.matcher(click_position_lw).find() ){
		   String dd = "";
	        dd = click_position_lw.replaceAll("\\D","_").replace("_+", "_");
	        //System.out.println(dd);
	         as = dd.split("_+");
		}
	        //System.out.println(as[1]);
		
		///System.exit(0);
		//click_position_lw.substring(0,aaa);
	    
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
		
	
		if (
			((click_position_lw.startsWith("s")) && (click_position_lw.length()>1) 
					&& (pattern.matcher(click_position_lw.substring(1,2)).matches()))
			|| (click_position_lw.startsWith("s")) && (click_position_lw.length()>1) 
			   &&  (pattern1.matcher(click_position_lw.substring(1,2)).matches())
            || (click_position_lw.startsWith("sst")) && (click_position_lw.length()>3) 
                && (pattern.matcher(click_position_lw.substring(3,4)).matches())
			||(click_position_lw.startsWith("ss")) && (click_position_lw.length()>2)
			&& (pattern.matcher(click_position_lw.substring(2,3)).matches())		
		    )
		   {
			//System.out.println(click_position_lw);
			source_type = "20";
		    browser_module = "10";	
		    if(prod_click_type_lw.length>0){
		       browser_type= prod_click_type_lw[0];
		    }
		    
		      browser_page = as[1];
		      browser_dtl_module =click_position_lw.substring(0,aaa);
   
		 }
	
		
	 if ((click_position_lw.startsWith("hp") ||(click_position_lw.startsWith("pu"))))
		{
    	  source_type = "00";
    	  browser_module = "00";
    	  browser_type= "01";
    	  browser_dtl_module = expo_order;
		}
       else if (click_position_lw.equals("gw"))
       {
    	   browser_module = "20";
    	   //99#20#99#0#99#99
    	  if(prod_click_type_lw.length>0){
    	   browser_type = prod_click_type_lw[0];
    	  }
		    
       }
	   else if (click_position_lw.equals("gp")) 
	   {
		   browser_module = "21";
//		   if (prod_click_type_lw==null || (prod_click_type_lw!=null&&prod_click_type_lw.length==0))
//		    {
		   if(prod_click_type_lw.length>0){
		     browser_type = prod_click_type_lw[0];
		   }
		    //}
	   }
	   else if (click_position_lw.equals("st")) 
	   {
		   source_type = "10";
		   browser_module = "99";
		   browser_type ="01";
//		   if (prod_click_type_lw==null || (prod_click_type_lw!=null&&prod_click_type_lw.length==0))
//		    {
		   if(prod_click_type_lw.length>0){
		      browser_type = prod_click_type_lw[0];
		   }
		   
		    //}
	   }
	    if ((click_position_lw.startsWith("sshot"))) 
	  {
		  source_type = "10";
		  browser_module = "50";//20150316根据模型修改由60改为50
		  browser_type ="01";
	  }
	   else  if  (click_position_lw.startsWith("ssreview"))	
	  {
		      source_type = "10";
			  browser_module = "51";
			  browser_type ="01";
		  
	  }
	  else if  (click_position_lw.startsWith("svh"))
	  {
	            source_type = "10";
				browser_module = "52";
				browser_type ="01";
			  
	  }
	    
	  else if  (click_position_lw.startsWith("cp") && 
			  (click_position_lw.indexOf("cpmy")<0) &&
			  (click_position_lw.indexOf("cpmf")<0))
	  {
				browser_module = "30";

				
				 if(prod_click_type_lw.length>0){
				    browser_type = prod_click_type_lw[0];
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
				  {  
					  source_type = "99";
				  
				  }
				  		  
	 }
			  
	  else if  (click_position_lw.equals("cpmy") || click_position_lw.equals("cpmf") 
			  || click_position_lw.startsWith("mydh"))
	  {
		  source_type = "70";
		  browser_module = "30";
          if(prod_click_type_lw.length>0){
		     browser_type = prod_click_type_lw[0];
          }

	  }
	  else if  (click_position_lw.startsWith("dcp"))
	  {   
		  source_type ="80";
		  browser_module = "99";
		  browser_type = "01";
	  }
	  else if  (click_position_lw.startsWith("listing"))
	  {   
		  source_type = "99";
		  browser_module = "60";
          if(prod_click_type_lw.length>0){
		     browser_type = prod_click_type_lw[0];
          }

	  }
	 
	  else if  (click_position_lw.startsWith("lp"))
	  {   
		  source_type ="60";
		  browser_module = "99";
          if(prod_click_type_lw.length>0){
		     browser_type = prod_click_type_lw[0];
          }

	      if (ext_id1!=null&&ext_id1.length>1)
           {
		      ext_id = ext_id1[1];
		   }
		  
	 }
	    
   if (source_type == "" || source_type == null || source_type.equals(""))
    { 
			source_type = "99";
		
	}

   
  if (browser_module == ""|| browser_module == null || browser_module.equals(""))
  { 
	browser_module = "99";
  }

  
  if (browser_type == ""|| browser_type == null || browser_type.equals("") ||browser_type.length()>2 )
  { 
	browser_type = "99";
  }


   if (browser_dtl_module == ""|| browser_dtl_module == null || browser_dtl_module.equals(""))
   { 
	   browser_dtl_module = "99";

    }

   if (browser_page == ""|| browser_page == null || browser_page.equals(""))
   { 
	  browser_page = "99";
	
   }
   

     return source_type + "#" + browser_module + "#"  
         + browser_type + "#" + site_in_out + "#" 
         + ext_id + "#" +browser_page;


	}
	

	

	 public static void main(String args[]) throws IOException
	 {
		// String test1 = "111";
		 FuncGetTypeCodeBrowser aa = new FuncGetTypeCodeBrowser();
		    //ref_url 引用url
			//click_position 浏览位置  对应#A-B-C|D的A部分,如s*
			//点击浏览类型prod_click_type    对应#A-B-C|D的C|D部分,如2|4554121
			//产品曝光排名expo_order
		 
		 System.out.println(aa.evaluate("http://www.dhgate.com/product/natural-skin-care-snail-essence-cream-hyaluronic/230345443.html","cppd","5|null:2","2"));
		 //System.out.println(test1);
	  }
}
