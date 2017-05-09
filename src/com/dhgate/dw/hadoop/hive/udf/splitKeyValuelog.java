	package com.dhgate.dw.hadoop.hive.udf;
	import java.util.ArrayList;

	import org.apache.commons.lang.StringUtils;
	import org.apache.hadoop.hive.ql.exec.UDF;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;

	//字符串：key分隔符value 大分隔符
	//大分隔符
	//分隔符
	//返回类型：1，返回key、2，返回value
	public class splitKeyValuelog extends UDF
	{
	    Text result = new Text();

	    ArrayList<String> strList = new ArrayList<String>();
	    String[] keyValueArray;

	    public Text evaluate(Text keyValue, Text bigSep, Text sep, IntWritable returnType,Text keyValue1)
	    {
	        if (keyValue == null)
	            return null;

	        strList.clear();
	        int sep_index = 0;
	        keyValueArray = keyValue.toString().split(bigSep.toString());
	        for (String str : keyValueArray)
	        {
	            // 返回key
	            if (returnType.get() == 1)
	                strList.add(str.split(sep.toString())[0]);
	            //System.out.println(str.split(sep.toString())[0]);
	            // 返回value1
	            else if (returnType.get() == 2 && keyValue1.toString().equals(str.split(sep.toString())[0]))
	            if(keyValue1.toString().equals(str.split(sep.toString())[0])){
	            	//System.out.println(keyValue1 + "3"+str);
	            	sep_index = str.indexOf(sep.toString());
	            	//System.out.println(str.substring(sep_index+1, str.length()));
	            	strList.add(str.substring(sep_index+1, str.length()));
	            //strList.add(str.split(sep.toString())[1]); 
	            }
	            else
	                return null;
	        }

	        result.set(StringUtils.join(strList.toArray(), bigSep.toString()));
	        return result;
	    }

	    public static void main(String args[])
	    {
	    	splitKeyValuelog t = new splitKeyValuelog();
	        System.out.println(t.evaluate(new Text("browser_version#40.0.2214.111\u001doperation_system#WINDOWS_VISTA\u001dcatepubid#004002\u001dbrowser#CHROME\u001dhostname#trackingapp02\u001dpic#215588780\u001dclkloc#s\u001dcid#100002006\u001dsupplierid#ff8080814757a1a30147bf82c73c061a\u001drc#1\u001dversion#ProductTest2015010"), new Text("\\035"), new Text("#"), new IntWritable(2), new Text("browser_version")));
	    }
	}
