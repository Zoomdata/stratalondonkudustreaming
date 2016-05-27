CREATE EXTERNAL TABLE default.strata_fruits_expanded
(                                                                                              
   _ts BIGINT,
   _id STRING,
   fruit STRING,
   country_code STRING,
   country_area_code STRING,
   phone_num STRING,
   message_date BIGINT,
   price FLOAT,
   keyword STRING
 )
 DISTRIBUTE BY HASH (_ts) INTO 120 BUCKETS                                
 TBLPROPERTIES (
 'kudu.table_name'='strata_fruits_expanded',
 'kudu.key_columns'='_ts,_id', 
 'kudu.master_addresses'='kudumaster:7051', 
 'kudu.num_tablet_replicas' = '3',
 'storage_handler'='com.cloudera.kudu.hive.KuduStorageHandler') 
