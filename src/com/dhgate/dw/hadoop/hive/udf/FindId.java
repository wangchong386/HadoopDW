package com.dhgate.dw.hadoop.hive.udf;
import org.apache.hadoop.hive.ql.exec.UDF;

public class FindId extends UDF
{
    String result = "";

    /*
     * 查询根据id得到字符类型的文章id
     */
    public String evaluate(String value, String find)
    {

        return this.getId3(value, find);
    }

    private String getId3(String value, String find)
    {
        if (value == null || value.equals(""))
        {
            return null;
        }

        if (value.indexOf(find + "=>") < 0)
            return null;

        if (value.split(find + "=>").length < 2)
            return "";

        result = value.split(find + "=>")[1];

        result = result.split(",")[0];

        return result;
    }
}
