# HadoopDW

平时使用的hive-udf

* 固定配置路径：
（由于我使用的是cdh5.5.1安装的hadoop集群以及hive,所有一下是我生产环境中的路径）

将hive udf打包好之后，上传到服务器中：	'/opt/cloudera/parcels/CDH-5.5.1-1.cdh5.5.1.p0.11/lib/hive/bin/.hiverc'

* 临时配置
如果只是其中一两个脚本用到该udf,可以直接将该udf直接固定在该脚本中，
```
add jar /dw/mds/search/udf/hive-contrib.jar;
CREATE TEMPORARY FUNCTION rowSequence AS 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';
```
--如下所示--
```
add jar /dw/mds/search/udf/hive-contrib.jar;
CREATE TEMPORARY FUNCTION rowSequence AS 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence';

INSERT OVERWRITE TABLE mds_search_keywords_dict_day PARTITION (dt='$etl_date')
select keywords_id, keywords_md5, keywords, create_time, flag from
(
select  keywords_id, keywords_md5, keywords, create_time, flag
        from tmp_mds_search_keywords_dict_day_02
        where flag = 0 and dt = '$etl_date'
union all
select  (rowSequence()+$max_keywords_id) as keywords_id, keywords_md5, keywords, create_time, flag
        from tmp_mds_search_keywords_dict_day_02
        where flag = 1 and dt = '$etl_date'
) tmp ;
drop temporary function rowSequence;
```

