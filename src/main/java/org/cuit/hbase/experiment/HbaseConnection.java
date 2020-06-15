package org.cuit.hbase.experiment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HbaseConnection {

    private Connection connection;
    private Admin admin;

    public HbaseConnection() {
        BasicConfigurator.configure();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","39.99.156.133");
        conf.set("hbase.zookeeper.property.clientPort","2181");

        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public  void ShowAllTables()throws  IOException {
        System.out.println("show all tables");
        TableName[] tableNames = admin.listTableNames();
        for (TableName ts : tableNames) {
            System.out.println(ts.toString());
        }
    }

    public boolean NamespaceExist(String name) throws IOException {
         NamespaceDescriptor[] namespaceDescriptors =  admin.listNamespaceDescriptors();
         for(NamespaceDescriptor NamespaceName :  namespaceDescriptors) {
             if(NamespaceName.getName().equals(name)) return true;
         }

         return false;
    }

    public boolean createNamespace(String name) throws IOException {
        if(NamespaceExist(name)) return false;
        try {
            NamespaceDescriptor namespace = NamespaceDescriptor.create(name).build();
            admin.createNamespace(namespace);
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean DeleteNamespace(String name) throws IOException {
        if(!NamespaceExist(name)) return false;
        try {
            admin.deleteNamespace(name);
            return true;
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean tableExists(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public  void DeleteTableByName(String tableName) throws IOException {
        if(!tableExists(tableName)) {
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    public void createTable(String tableName, String... families) throws Exception{

        if(tableExists(tableName)) {

            System.out.println(tableName + " already exist!");
            return ;
        }

        HTableDescriptor tableDescriptor=new HTableDescriptor(TableName.valueOf(tableName));
        try{
            for(String family:families) {
                tableDescriptor.addFamily(new HColumnDescriptor(family));
            }
            admin.createTable(tableDescriptor);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public void AddInfo(String tableName, Put p) throws IOException {
        Table table = GetTable(tableName);
        table.put(p);
    }

    public void PutData(String tableName, String rowKey, String family, String data, String info) {
        Put put = new Put(rowKey.getBytes()); //指定rowKey
        put.addColumn(family.getBytes(), data.getBytes(), info.getBytes());
        try {
            AddInfo(tableName, put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Table GetTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }


    public ResultScanner scanData(String tableName) throws IOException {
        Scan scan = new Scan();
        Table table = GetTable(tableName);
        return table.getScanner(scan);
    }

    public void deleteRowKey(String tableName, String rowKey) throws IOException {
        Table table = GetTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    public Map<byte[], byte[]> getFamily(String tableName, String rowKey, String family) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = GetTable(tableName);
        Result result = table.get(get);
        return result.getFamilyMap(Bytes.toBytes(family));
    }

    public int getFamilyCount(String tableName, String rowKey, String family) throws IOException {
        return getFamily(tableName, rowKey, family).size();
    }

    //删除指定cell数据
    public void deleteByRowKeyCell(String tableName, String rowKey, String family, String con) throws IOException {

        Table table = GetTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(con));
        table.delete(delete);
    }

}