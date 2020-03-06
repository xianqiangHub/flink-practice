package com.bigdata.etl.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HbaseUtils {
    /**
     * 日志记录
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseUtils.class);

    /**
     * Hbase 连接对象
     */
    private static Connection CONN;

    /**
     * 获取Hbase的连接
     *
     * @return Hbase connection
     * @throws Exception the exception
     */
    public synchronized static Connection getConnection() throws Exception {
        if (null == CONN || CONN.isClosed()) {
            try {
                Configuration conf = HBaseConfiguration.create();
                CONN = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                LOGGER.error("can not establish hbase connection.", e);
                throw new Exception("can not establish hbase connection.", e);
            }
        }
        return CONN;
    }

    /**
     * 创建命名空间
     *
     * @param namespace 命名空间
     * @throws Exception Exception
     */
    public static void createNamespace(String namespace) throws Exception {
        Admin admin = null;
        try {
            admin = HbaseUtils.getConnection().getAdmin();
            if (HbaseUtils.namespaceIsExists(admin, namespace)) {
                LOGGER.warn("The namespace " + namespace + " already exists !");
                return;
            }
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
            LOGGER.info("create namespace " + namespace + " seccuss.");
        } finally {
            HbaseUtils.closeAdmin(admin);
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName tableName
     * @return true:存在, false:不存在
     * @throws Exception Exception
     */
    public static boolean tableExists(String tableName) throws Exception {
        Admin admin = null;
        try {
            admin = HbaseUtils.getConnection().getAdmin();
            return admin.tableExists(TableName.valueOf(tableName));
        } finally {
            HbaseUtils.closeAdmin(admin);
        }
    }

    /**
     * 创建一个表，这个表没有任何region
     *
     * @param tableName 表名
     * @param cfs       列族
     * @throws Exception Exception
     */
    public static void createTable(String tableName, String... cfs) throws Exception {
        Admin admin = null;
        try {
            admin = HbaseUtils.getConnection().getAdmin();
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String family : cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hTableDescriptor.addFamily(hColumnDescriptor);
                hColumnDescriptor.setMaxVersions(3);
            }
            admin.createTable(hTableDescriptor);
            LOGGER.info("create table " + tableName + " seccuss.");
        } finally {
            HbaseUtils.closeAdmin(admin);
        }
    }

    /**
     * 清空表数据, 保留分区
     *
     * @param tableName 表名
     * @throws Exception Exception
     */
    public static void truncateTable(String tableName) throws Exception {
        Admin admin = null;
        TableName tableNameObj = TableName.valueOf(tableName);
        try {
            admin = HbaseUtils.getConnection().getAdmin();
            if (!admin.tableExists(tableNameObj)) {
                LOGGER.error("The table " + tableName + " does not exists!");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.truncateTable(tableNameObj, true);
        } finally {
            HbaseUtils.closeAdmin(admin);
        }
    }

    /**
     * 获取hbase表中的列族字段
     *
     * @param tableName 表名
     * @return 列族字段集合 family fields
     * @throws Exception Exception
     */
    public static List<String> getFamilyFields(String tableName) throws Exception {
        Admin admin = null;
        List<String> families = new LinkedList<>();
        try {
            admin = HbaseUtils.getConnection().getAdmin();
            HTableDescriptor hTableDesc = admin.getTableDescriptor(TableName.valueOf(tableName));
            hTableDesc.getFamilies().forEach(desc -> families.add(desc.getNameAsString()));
            return families;
        } finally {
            HbaseUtils.closeAdmin(admin);
        }
    }

    /**
     * 追加新的列族
     *
     * @param tableName tableName
     * @param families  families
     * @throws Exception Exception
     */
    public static void addColumnFamily(String tableName, String... families) throws Exception {
        Admin admin = null;
        try {
            admin = HbaseUtils.getConnection().getAdmin();
            for (String family : families) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
                admin.addColumn(TableName.valueOf(tableName), columnDescriptor);
            }
        } finally {
            HbaseUtils.closeAdmin(admin);
        }
    }


    /*
* 查询hbase表
*
* @tableName 表名
*/
    public static ResultScanner getResult(Table table, Scan scan) throws Exception {
        //Table table = null;
        ResultScanner rs = null;
        try {
            //table = HbaseUtils.getConnection().getTable(TableName.valueOf(tableName));
            rs = table.getScanner(scan);
        } catch (Exception e) {
            LOGGER.error("批量读取数据失败！", e);
            throw new Exception("批量读取数据失败！", e);
        } finally {
            closeTable(table);
        }
        return rs;
    }

    /**
     * 批量插入数据
     *
     * @param table
     * @param puts  List<Put>
     * @throws Exception Exception
     */
    public static boolean batchPuts(Table table, List<Put> puts) throws Exception {
        //Table table = null;
        try {
            //table = HBaseUtils.getConnection().getTable(TableName.valueOf(tableName));
            table.put(puts);
        } catch (Exception e) {
            LOGGER.error("批量存储数据失败！", e);
            throw new Exception("批量存储数据失败！", e);
        } finally {
            closeTable(table);
        }

        return true;
    }

    /**
     * 多线程批量插入hbase
     *
     * @param tableName 表名
     * @param puts      List<Put>
     */
    public static void batchPut(final String tableName, List<Put> puts) {
        ExecutorService pool = Executors.newFixedThreadPool(5);
        pool.submit(() -> {
            BufferedMutator mutator = null;
            try {
                Connection conn = HbaseUtils.getConnection();
                //HBaseUtils.enableTable(tableName);
                BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
                params.writeBufferSize(5 * 1024 * 1024);
                mutator = conn.getBufferedMutator(params);
                mutator.mutate(puts);
                mutator.flush();
            } catch (Exception e) {
                LOGGER.error("write data to hbase failed!", e);
            } finally {
                try {
                    assert null != mutator;
                    mutator.close();
                } catch (IOException e) {
                    LOGGER.error("close mutator failed", e);
                }
            }
        });
    }

    /**
     * 判断命名空间是否存在
     *
     * @param admin     Admin
     * @param namespace 命名空间
     * @return true:存在、false:不存在
     * @throws Exception Exception
     */
    private static boolean namespaceIsExists(Admin admin, String namespace) throws Exception {
        NamespaceDescriptor[] namespaceDescs = admin.listNamespaceDescriptors();
        List<String> ns = new LinkedList<>();
        Arrays.stream(namespaceDescs).forEach(namespaceDesc -> ns.add(namespaceDesc.getName()));

        return ns.contains(namespace);
    }

    /**
     * 启用表, 若表状态为disable使其状态变为enable
     *
     * @param tableName 表名
     * @throws Exception Exception
     */
    private static void enableTable(String tableName) throws Exception {
        // 若表是disable状态, 则启用表
        Admin admin = HbaseUtils.getConnection().getAdmin();
        if (admin.isTableAvailable(TableName.valueOf(tableName))) {
            LOGGER.info("The table " + tableName + " is available !");
            return;
        }
        admin.enableTable(TableName.valueOf(tableName));
        LOGGER.info("enable talbe " + tableName + " seccuss.");
    }

    /**
     * 刷新表空间
     *
     * @param tableName tableName
     * @throws Exception Exception
     */
    public static void flushTable(String tableName) throws Exception {
        Admin admin = null;
        try {
            admin = HbaseUtils.getConnection().getAdmin();
            admin.flush(TableName.valueOf(tableName));
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            HbaseUtils.closeAdmin(admin);
        }
    }

    /**
     * 关闭hbase表管理对象（DDL）的Admin
     *
     * @param admin hbase表管理对象
     */
    public static void closeAdmin(Admin admin) {
        if (null != admin) {
            try {
                admin.close();
            } catch (IOException e) {
                LOGGER.error("close connection failure !", e);
            }
        }
    }

    /**
     * 关闭table
     *
     * @param table 表对象
     */
    public static void closeTable(Table table) {
        if (null != table) {
            try {
                table.close();
            } catch (IOException e) {
                LOGGER.error("close table failure !", e);
            }
        }
    }

    /**
     * 关闭hbase连接
     */
    public static void closeConn() {
        if (null != CONN) {
            try {
                CONN.close();
            } catch (IOException e) {
                LOGGER.error("close connection failure !", e);
            }
        }
    }
}
