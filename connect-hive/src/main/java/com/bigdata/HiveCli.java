package com.bigdata;

//目前支持的 Hive 版本包括 2.3.4 和 1.2.1

/**
 * 下面列出的是在 1.9.0 中已经支持的功能：
 * <p>
 * 提供简单的 DDL 来读取 Hive 元数据，比如 show databases、show tables、describe table 等。
 * 可通过 Catalog API 来修改 Hive 元数据，如 create table、drop table 等。
 * 读取 Hive 数据，支持分区表和非分区表。
 * 写 Hive 数据，支持非分区表。
 * 支持 Text、ORC、Parquet、SequenceFile 等文件格式。
 * 支持调用用户在 Hive 中创建的 UDF。
 * 由于是试用功能，因此还有一些方面不够完善，下面列出的是在 1.9.0 中缺失的功能：
 * <p>
 * 不支持INSERT OVERWRITE。
 * 不支持写分区表。
 * 不支持ACID表。
 * 不支持Bucket表。
 * 不支持View。
 * 部分数据类型不支持，包括Decimal、Char、Varchar、Date、Time、Timestamp、Interval、Union等。
 */
public class HiveCli {

    public static void main(String[] args) {

//        String name = "myhive";
//        String defaultDatabase = "default";
//        String hiveConfDir = "/path/to/hive_conf_dir";
//        String version = "2.3.4";
//
//        TableEnvironment tableEnv = …; // create TableEnvironment
//        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase,
//                hiveConfDir, version);
//        tableEnv.registerCatalog(name, hiveCatalog);
//        tableEnv.useCatalog(name);
//
//
//        TableEnvironment tableEnv = …; // create TableEnvironment
//        tableEnv.registerCatalog("myhive", hiveCatalog);
//// set myhive as current catalog
//        tableEnv.useCatalog("myhive");
//
//        Table src = tableEnv.sqlQuery("select * from src");
//// write src into a sink or do further analysis
//……
//
//        tableEnv.sqlUpdate("insert into src values ('newKey', 'newVal')");
//        tableEnv.execute("insert into src");

    }
}
