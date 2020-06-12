package org.cuit.hbase.experiment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    public Table GetTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }


    public ResultScanner scanData(String tableName) throws IOException {
        Scan scan = new Scan();
        Table table = GetTable(tableName);
        //        printResult(resultScanner);
        return table.getScanner(scan);
    }

    //根据级数查询
    public void scanDeep(String tableName, int deep) throws IOException {
        ResultScanner resultScanner = scanData(tableName);
        for(Result result : resultScanner) {
            String rowKey = Bytes.toString(result.getRow());
            Integer td = Integer.parseInt(rowKey.substring(0, rowKey.indexOf("_")));
            if(td.equals(deep)) {
                System.out.print(rowKey+"   ");
                String name = Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("name")));
                String fid = Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("fid")));
                System.out.print(name+"    ");
                if(fid != null)
                    System.out.print(fid+"    ");
                System.out.println();
            }
        }
    }

    //根据rowKey查询子部门
    public List<String> scanByRowKey(String tableName, String rowKey) throws IOException {
        List<String> childIdList = new ArrayList<String>();
        System.out.println("查询"+ rowKey+"的子部门信息");
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = GetTable(tableName);
        Result result = table.get(get);
        if(result == null) {
            return null;
        }
        String name = Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("name")));
        String fid = Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("fid")));
        if(fid != null)
            System.out.print(name+"    ");
        String NumStr = Bytes.toString(result.getValue(Bytes.toBytes("subdept"),
                Bytes.toBytes("childNum")));
        if(NumStr != null)
            System.out.println("有" + NumStr+"个子部门");
        else {
            System.out.println("没有子部门");
            return null;
        }
        int childNum = Integer.parseInt(NumStr);
        for(int i = 1; i <= childNum; i++) {
            String childId = Bytes.toString(result.getValue(Bytes.toBytes("subdept"),
                    Bytes.toBytes("child_"+String.valueOf(i)+"_id")));
            System.out.println(childId);
            childIdList.add(childId);
        }
        return childIdList;
    }
    public void deleteRowKey(String tableName, String rowKey) throws IOException {
        Table table = GetTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    public int getRowKeyChildNum(String tableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = GetTable(tableName);
        Result result = table.get(get);
        String NumStr = Bytes.toString(result.getValue(Bytes.toBytes("subdept"),
                Bytes.toBytes("childNum")));
        if(NumStr == null) {
            Put put = new Put(rowKey.getBytes()); //指定rowKey
            put.addColumn("subdept".getBytes(), "childNum".getBytes(), String.valueOf(0).getBytes());
            AddInfo(tableName, put);
            return 0;
        }
        else return Integer.parseInt(NumStr);
    }

}