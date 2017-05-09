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

public class JdbcToHiveTest {
	
	
	 //hive server 1的driver classname是org.apache.hadoop.hive.jdbc.HiveDriver，
	 //Hive Server 2的是org.apache.hive.jdbc.HiveDriver
	
        //private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
        private static String driverName = "org.apache.hive.jdbc.HiveDriver";
        //private static String url = "jdbc:hive://192.168.11.157:10000/default";
        private static String url = "jdbc:hive2://10.21.100.87:10000/default";
        //jdbc:mysql://dw-datanode01:3306/hivemetadb?useUnicode=true&amp;characterEncoding=UTF-8
        private static String user = "hdfs";
        private static String password = "hdfs";
        private static String sql = "";
        private static ResultSet res;
        private static final Logger log = Logger.getLogger(JdbcToHiveTest.class);

        public static void main(String[] args) throws Exception {
                try {

                        Class.forName(driverName);
                        Connection conn = DriverManager.getConnection(url, user, password);
                        Statement stmt = conn.createStatement();
                        // 创建的表名
                        String tableName = "testHiveDriverTable";
                        /** 第一步:存在就先删除 **/
                        sql = "drop table if exists " + tableName;
                        stmt.execute(sql);
                        System.out.println("第一步:存在就先删除");
                        /** 第二步:不存在就创建 **/
                        sql = "create table " + tableName + " (key int, value string)  row format delimited fields terminated by '|'";
                        stmt.execute(sql);
                        System.out.println("第二步:不存在就创建");

                        // 执行“show tables”操作
                        sql = "show tables '" + tableName + "'";
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("执行“show tables”运行结果:");
                        if (res.next()) {
                                System.out.println(res.getString(1));
                        }
                        

                        // 执行“describe table”操作
                        sql = "describe " + tableName;
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("执行“describe table”运行结果:");
                        while (res.next()) {  
                                System.out.println(res.getString(1) + "\t" + res.getString(2));
                        }

                        // 执行“load data into table”操作
                        //String filepath = "/home/hadoop/ziliao/userinfo.txt";
                        String filepath = "/tmp/test.txt";
                        sql = "load data local inpath '" + filepath + "' into table " + tableName;
                        System.out.println("Running:" + sql);
                        //stmt.execute(sql);
                        
                        tableName = "ods_mob_visit_log_wap";
                        // 执行“select * query”操作
                        sql = "select * from " + tableName + " limit 100";
                        System.out.println("Running:" + sql);
                        res = stmt.executeQuery(sql);
                        System.out.println("执行“select * query”运行结果:");
                        while (res.next()) {
                                System.out.println(res.getString(1) + "\t" + res.getString(2));
                        }

                        // 执行“regular hive query”操作
                        sql = "select count(1) as test from " + tableName;
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