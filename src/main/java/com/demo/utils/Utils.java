package com.demo.utils;

import java.io.InputStream;
import java.util.Properties;

public class Utils {
	
	/**
	 * 根据配置文件key值获取value值
	 * @param key
	 * @return
	 */
	public static String getProp(String key) {
		// 读取配置文件
		Properties pro = new Properties();
		InputStream in = Utils.class.getClassLoader().getResourceAsStream("paramConfig.properties");
		String valStr = "";
		try {
			pro.load(in);
			valStr = pro.getProperty(key);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return valStr;
	}
}
