/**
 * 
 */
package com.dhgate.dw.hadoop.hive.example.jdbc;

/**
 * @author longyin
 *
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import org.apache.log4j.Logger;

import org.apache.log4j.Logger;

public class JdbcToHiveTestSelect {
	
	
	 //hive server 1的driver classname是org.apache.hadoop.hive.jdbc.HiveDriver，
	 //Hive Server 2的是org.apache.hive.jdbc.HiveDriver
	
        //private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
        //private static String url = "jdbc:hive://192.168.11.157:10000/default";
	
	    //驱动类，目前仓库使用的Hive类型为HiveServer2,明细版本为0.12
        private static String driverName = "org.apache.hive.jdbc.HiveDriver";
        //HiveServer2的URL，主机为10.21.100.87
        private static String url = "jdbc:hive2://10.21.100.87:10000/default";
       //访问的用户和密码
        private static String user = "hdfs";
        private static String password = "hdfs";
        private static String sql = "";
        private static ResultSet res;
        private static final Logger log = Logger.getLogger(JdbcToHiveTestSelect.class);

        public static void main(String[] args) throws Exception {
                try {

                        Class.forName(driverName);
                        Connection conn = DriverManager.getConnection(url, user, password);
                        Statement stmt = conn.createStatement();
                    
                        //要访问的名表
                        String tableName = "ods_mob_visit_log_wap";
                        
                        // 执行“show tables”操作
                        sql = "show tables '" + tableName + "'";
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("执行“show tables”运行结果:");
                        if (res.next()) {
                                System.out.println(res.getString(1));
                        }
                        

                        // 执行“describe table”操作查看表结构
                        sql = "describe " + tableName;
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("执行“describe table”运行结果:");
                        while (res.next()) {  
                                System.out.println(res.getString(1) + "\t" + res.getString(2));
                        }

    
                        // 执行“select * query”操作，dt为时间分区，取前50条
                        sql = "select * from " + tableName + " where dt = '2014-11-21' limit 50";
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("执行“select * query”运行结果，第一列和第二列的值为:");
                        while (res.next()) {
                                System.out.println(res.getString(1) + "\t" + res.getString(2));
                        }

                        // 执行“regular hive query”操作，查询某一天的总数
                        sql = "select count(1) from " + tableName  + " where dt = '2014-11-21' ";
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("执行“regular hive query”运行结果:");
                        while (res.next()) {
                                System.out.println(res.getString(1));

                        }

                        conn.close();
                        conn = null;
                        
                } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                       // log.error(driverName + " not found!", e);
                        //System.exit(1);
                } catch (SQLException e) {
                        e.printStackTrace();
                       //log.error("Connection error!", e);
                        //System.exit(1);
                }catch (Exception e) {
                    e.printStackTrace();
                   //log.error("Connection error!", e);
                    //System.exit(1);
                }


        }
}