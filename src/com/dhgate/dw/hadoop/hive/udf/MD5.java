package com.dhgate.dw.hadoop.hive.udf;

import java.security.NoSuchAlgorithmException;

public class MD5 {

	/**
	 * 获取字符串的Md5码
	 * 
	 * @param str
	 *            　文件路径
	 * @return
	 */
	public static String getMd5ByString(String str) {
		String inStr = str;
		java.security.MessageDigest md = null;
		String out = null;

		try {
			md = java.security.MessageDigest.getInstance("MD5");
			byte[] digest = md.digest(inStr.getBytes());
			out = byte2hex(digest);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return "";
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
		
		return out;
	}

	/**
	 * 整理成32位大写的MD5
	 * 
	 * @param b
	 * @return
	 */
	private static String byte2hex(byte[] b) {
		String hs = "";
		String stmp = "";
		for (int n = 0; n < b.length; n++) {
			stmp = (java.lang.Integer.toHexString(b[n] & 0XFF));
			if (stmp.length() == 1) {
				hs = hs + "0" + stmp;
			} else {
				hs = hs + stmp;
			}
		}
		return hs.toUpperCase();
	}

	public static void main(String[] args) {
		try {
			System.out.println(getMd5ByString("中 国 人"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
