package com.dhgate.dw.hadoop.hive.udf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.hadoop.hive.ql.exec.UDF;

public class FuncUrlDecoder extends UDF {

	/**
	 * url decode 
	 * 
	 * @param value
	 * @return
	 */
	public String evaluate(String value) {
		try {
			return URLDecoder.decode(value, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return value;
	}

	public String evaluate(String value, int count) {
		for (int i = 0; i < count; i++) {
			try {
				value = URLDecoder.decode(value,"UTF-8");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return value;
	}
}
