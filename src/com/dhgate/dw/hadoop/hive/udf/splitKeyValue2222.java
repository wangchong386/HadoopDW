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
public class splitKeyValue2222 extends UDF
{
    Text result = new Text();

    ArrayList<String> strList = new ArrayList<String>();
    String[] keyValueArray;

    public Text evaluate(Text keyValue, Text bigSep, Text sep, IntWritable returnType,Text keyValue1)
    {
        if (keyValue == null)
            return null;

        strList.clear();
        keyValueArray = keyValue.toString().split(bigSep.toString());
        for (String str : keyValueArray)
        {
            // 返回key
            if (returnType.get() == 1)
                strList.add(str.split(sep.toString())[0]);
            //System.out.println(str.split(sep.toString())[0]);
            // 返回value1
            else if (returnType.get() == 2 && keyValue1.toString().equals(str.split(sep.toString())[0]))
        	    //if(str.split(sep.toString())[0]).indexOf(keyValue1.toString())>1{
            	//System.out.println(str.split(sep.toString())[1]);
        	    //System.out.println(str.split(sep.toString())[1]);
            	if(keyValue1.toString().equals(str.split(sep.toString())[0]))
            	{
                	strList.add(str.split(sep.toString())[1]); 
            	}
            else
                return null;
        }

        result.set(StringUtils.join(strList.toArray(), bigSep.toString()));
        return result;
    }

    public static void main(String args[])
    {
    	splitKeyValue2222 t = new splitKeyValue2222();
        System.out.println(t.evaluate(new Text("schtype#ks,sssss#asaas,asdsadas#12121212"), new Text(","), new Text("#"), new IntWritable(2), new Text("sssss")));
    }
}












