package com.dhgate.dw.hadoop.hive.example.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

    //@Description("")

public class FuncGetSellerSourceUDF  extends UDF{
	
	
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
	


	public String evaluate(String param1,String param2){
		
		 if(null != param2 && !param2.equals("")){
			 return param2;
		 }
		 
		 String lowerParam1;
		 if(null != param1 && !param1.equals("")){
			 lowerParam1 = param1.toLowerCase();
		 }else{
			 return "Null";
		 }

		 if(lowerParam1.equals("bbs")){ //bbs
			 return "BBS";
		 }
		 if(lowerParam1.startsWith("hd") || lowerParam1.indexOf("huodong")>-1){  //hd%   %huodong%  &&and ||or
			 return "活动页面";
		 }
         if ( lowerParam1.indexOf("beizhan")>-1 ) {
             return  "Beizhan";                                                    
             }
            if ( lowerParam1.startsWith("edm") ) {
             return  "EDM";    
              }
            if ( lowerParam1.startsWith("sd") ) {
             return  "Google网盟";     
               }
            if ( lowerParam1.startsWith("baidu") ) {
             return  "百度";   
               }
            if ( lowerParam1.startsWith("cpa|") ) {
             return  "CPA"; 
              }
            if ( lowerParam1.startsWith("qq") ) {
             return  "QQ群";  
             }
            if ( lowerParam1.indexOf("fob")>-1 ||
                 lowerParam1.equals("pxwaimao") ||
                 lowerParam1.indexOf("szfob")>-1 ||
                 lowerParam1.equals("youchengchemical") ||
                 lowerParam1.equals("spring") ||
                 lowerParam1.equals("jsiec") ||
                 lowerParam1.equals("cnexp") ||
                 lowerParam1.equals("globalimporter") ||
                 lowerParam1.equals("yicer") ||
                 lowerParam1.equals("tradeidc") ||
                 lowerParam1.indexOf("99fob")>-1 ) {
                 return  "外贸论坛";                                                        
              }
            if ( lowerParam1.equals("sellgreat") ||
                 lowerParam1.equals("onccc") ||
                 lowerParam1.equals("net114") ||
                 lowerParam1.equals("jincheng") ||
                 lowerParam1.equals("jinbao") ||
                 lowerParam1.equals("isohoo") ||
                 lowerParam1.equals("wdzxiu") ||
                 lowerParam1.equals("tdc2c") ||
                 lowerParam1.equals("pifa588") ||
                 lowerParam1.equals("16daixiao") ||
                 lowerParam1.equals("pflm") ||
                 lowerParam1.equals("mywebc") ||
                 lowerParam1.equals("cnshop") ||
                 lowerParam1.equals("hao5d") ||
                 lowerParam1.equals("sousoo") ||
                 lowerParam1.equals("chyee") ||
                 lowerParam1.equals("by35") ||
                 lowerParam1.equals("tbjie") ||
                 lowerParam1.equals("taokec") ||
                 lowerParam1.equals("taobaoshangcheng") ||
                 lowerParam1.equals("wangdian8") ||
                 lowerParam1.equals("zhifucn") ||
                 lowerParam1.equals("kaigewangdian") ||
                 lowerParam1.equals("i180") ||
                 lowerParam1.equals("topbao") ||
                 lowerParam1.equals("eb228") ||
                 lowerParam1.equals("qncye") ||
                 lowerParam1.equals("wschn") ||
                 lowerParam1.equals("fjego") ||
                 lowerParam1.equals("taobaotuangou") ||
                 lowerParam1.equals("dianmeng") ||
                 lowerParam1.equals("daixiao114") ||
                 lowerParam1.equals("daixiaozj") ||
                 lowerParam1.equals("hackhero") ||
                 lowerParam1.equals("86wd") ||
                 lowerParam1.equals("youhuoyuan") ||
                 lowerParam1.equals("onedian") ||
                 lowerParam1.equals("yubaibai") ||
                 lowerParam1.equals("16huoyuan") ||
                 lowerParam1.equals("yaotaohuo") ||
                 lowerParam1.equals("eshop2") ||
                 lowerParam1.equals("cnshop8") ||
                 lowerParam1.equals("163er") ||
                 lowerParam1.equals("huo66") ||
                 lowerParam1.equals("5idaili") ||
                 lowerParam1.equals("321eee") ||
                 lowerParam1.equals("58555") ||
                 lowerParam1.equals("59ikan") ||
                 lowerParam1.equals("chinawsw") ||
                 lowerParam1.equals("e9898") ||
                 lowerParam1.equals("cnwangshang") ||
                 lowerParam1.equals("shopdn") ||
                 lowerParam1.equals("showshops") ||
                 lowerParam1.equals("myibaba") ||
                 lowerParam1.equals("666nnn") ||
                 lowerParam1.equals("18157") ||
                 lowerParam1.equals("alihyw") ||
                 lowerParam1.equals("riao") ||
                 lowerParam1.equals("china151") ||
                 lowerParam1.equals("tiaohuo") ||
                 lowerParam1.equals("zpooo") ||
                 lowerParam1.equals("taobaodl") ||
                 lowerParam1.equals("k235") ||
                 lowerParam1.equals("17daili") ||
                 lowerParam1.equals("pifa7") ||
                 lowerParam1.equals("huoyuan") ||
                 lowerParam1.equals("zgddp") ||
                 lowerParam1.equals("idaili") ||
                 lowerParam1.equals("52youa") ||
                 lowerParam1.equals("hhuoyuan") ||
                 lowerParam1.equals("shanzhaizhu") ||
                 lowerParam1.equals("studentboss") ||
                 lowerParam1.equals("wabei") ||
                 lowerParam1.equals("cg01") ||
                 lowerParam1.equals("dzwang") ||
                 lowerParam1.equals("53daixiao") ||
                 lowerParam1.equals("17daili") ||
                 lowerParam1.equals("18daixiao") ||
                 lowerParam1.equals("dxpflm") ||
                 lowerParam1.equals("daohang114") ||
                 lowerParam1.equals("zhaowangdian") ||
                 lowerParam1.equals("dataodu") ||
                 lowerParam1.equals("qpstar") ||
                 lowerParam1.equals("kooaoo") ||
                 lowerParam1.equals("ywzj") ||
                 lowerParam1.equals("dg66") ||
                 lowerParam1.equals("pengyouwo") ||
                 lowerParam1.equals("clubzj") ||
                 lowerParam1.equals("tobeno1") ||
                 lowerParam1.equals("51peijian") ||
                 lowerParam1.equals("sjpj") ||
                 lowerParam1.equals("858a") ||
                 lowerParam1.equals("qiaock") ||
                 lowerParam1.equals("huoyuanpu") ||
                 lowerParam1.equals("aligg") ||
                 lowerParam1.equals("watchdoor") ||
                 lowerParam1.equals("ledb2b") ||
                 lowerParam1.equals("bags163") ||
                 lowerParam1.equals("hhyls") ||
                 lowerParam1.equals("shopdidai") ||
                 lowerParam1.equals("kezhan8") ||
                 lowerParam1.equals("y13") ||
                 lowerParam1.equals("huoyuan808") ||
                 lowerParam1.equals("51dxpf") ||
                 lowerParam1.equals("511huoyuan") ||
                 lowerParam1.equals("wanwubao") ||
                 lowerParam1.equals("hao123cn") ||
                 lowerParam1.equals("668n") ||
                 lowerParam1.equals("yougood") ||
                 lowerParam1.equals("29cy") ||
                 lowerParam1.equals("yinjiasm") ||
                 lowerParam1.equals("u19") ||
                 lowerParam1.equals("yn213") ||
                 lowerParam1.equals("pifatong") ||
                 lowerParam1.equals("888a") ||
                 lowerParam1.equals("40t") ||
                 lowerParam1.equals("34131com") ||
                 lowerParam1.equals("taskcn") ||
                 lowerParam1.equals("28b2b") ||
                 lowerParam1.equals("miss8") ||
                 lowerParam1.equals("ynshangji") ||
                 lowerParam1.equals("koduo") ||
                 lowerParam1.equals("57616com") ||
                 lowerParam1.equals("168mz") ||
                 lowerParam1.equals("bossqh") ||
                 lowerParam1.equals("ali002") ||
                 lowerParam1.equals("3s4u") ||
                 lowerParam1.equals("idaili") ||
                 lowerParam1.equals("huoyuanba") ) {
             return  "线上联盟";       
              }
            if ( lowerParam1.startsWith("seo|") ||
                 lowerParam1.startsWith("txd|") ||
                 lowerParam1.startsWith("ced|") ) {
             return  "SEO"; 
             }
            if ( lowerParam1.startsWith("bm|") ) {
             return  "买家PPC";  
             }
            if ( lowerParam1.indexOf("iscs")>-1 ) {
             return  "ISCS"; 
             }
            if ( lowerParam1.startsWith("qd") ) {
             return  "渠道";
             }
            if ( lowerParam1.startsWith("info") ) {
             return  "开放平台(新)";
             }
            if ( lowerParam1.startsWith("cm") ) {
             return  "CM精准招募(新)";  
             }
            if ( lowerParam1.startsWith("zs") ) {
             return  "线下招募";
             }
            if ( lowerParam1.startsWith("dg") ) {
             return  "东莞项目";
             }
            if ( lowerParam1.startsWith("ningbo") ) {
             return  "宁波项目"; 
             }
            if ( lowerParam1.startsWith("ww") ) {
             return  "旺旺群";
             }
            if ( lowerParam1.startsWith("peixun") ) {
             return  "培训"; 
             }
            if ( lowerParam1.startsWith("saa") ) {
             return  "SAA"; 
             }
            if ( lowerParam1.startsWith("gz") ) {
             return  "广州项目"; 
             }
            if ( lowerParam1.startsWith("newsaagz") ) {
             return  "SAA广州"; 
             }
            if ( lowerParam1.startsWith("newsaadg") ) {
             return  "SAA东莞"; 
             }
            if ( lowerParam1.startsWith("newsaabj") ) {
             return  "SAA总部";
            }
           
		 return "Others";
	}
	
	
	
	public static void main(String args[]){
		FuncGetSellerSourceUDF test = new FuncGetSellerSourceUDF();
		System.out.println(test.evaluate("taobaotuangou", null));
		System.out.println(test.evaluate("taobaotuangou", "args2"));
	}
	
	

}
