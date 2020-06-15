package org.cuit.hbase.experiment;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class experiment3 {
    private int num;
    private static final int MaxNum = 30;

    private final HbaseConnection con;
    private HashMap<Integer, Integer>numMp;

    public experiment3() {
        con = new HbaseConnection();
    }

    public void createDept() throws Exception {
        if(con.tableExists("dept")) {
            con.DeleteTableByName("dept");
        }

        con.createTable("dept", "base", "subdept");

        num = 0;
        numMp = new HashMap<>();
        for(int i = 0; i <= MaxNum; i++) {
            numMp.put(i, 0);
        }
        dfs("0_000");
    }

    //根据级数查询
    public void scanDeep(int deep) throws IOException {
        ResultScanner resultScanner = con.scanData("dept");
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
    public List<String> scanByRowKey(String rowKey) throws IOException {
        List<String> childIdList = new ArrayList<String>();
        System.out.println("查询"+ rowKey+"的子部门信息");
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = con.GetTable("dept");
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

    public int getRowKeyChildNum( String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = con.GetTable("dept");
        Result result = table.get(get);
        String NumStr = Bytes.toString(result.getValue(Bytes.toBytes("subdept"),
                Bytes.toBytes("childNum")));
        if(NumStr == null) {
            Put put = new Put(rowKey.getBytes()); //指定rowKey
            put.addColumn("subdept".getBytes(), "childNum".getBytes(), String.valueOf(0).getBytes());
            con.AddInfo("dept", put);
            return 0;
        }
        else return Integer.parseInt(NumStr);
    }

    public String addDep(String faRowKey, String name) throws IOException {
        int cnt = getRowKeyChildNum(faRowKey);
        int deep = Integer.parseInt(faRowKey.substring(0, faRowKey.indexOf("_"))) + 1;
        Random random = new Random();
        String chRowKey = deep+"_" + random.nextInt(9999)+9721;
        //在1_001节点上加上一个新的节点
        con.PutData("dept", faRowKey, "subdept", "child_"+String.valueOf(cnt+1) +"_id", chRowKey);
        con.PutData("dept", faRowKey, "subdept", "childNum", String.valueOf(cnt+1));
        con.PutData("dept", chRowKey, "base", "name", name);
        con.PutData("dept", chRowKey, "base", "fid", faRowKey);
        return chRowKey;
    }

    public void deleteDep(String toRowKey, String nowRowKey) throws IOException {
        List<String> childList = scanByRowKey(nowRowKey);
        int cnt = getRowKeyChildNum(toRowKey);
        if(childList == null) return;
        for(String id : childList) {
            cnt++;
            con.PutData("dept", toRowKey, "subdept", "child_"+String.valueOf(cnt)+"_id", id);
            con.PutData("dept", id, "base", "fid", toRowKey);
        }
        //维护子部门数
        con.PutData("dept", toRowKey, "subdept", "childNum", String.valueOf(cnt));
        con.deleteRowKey("dept", nowRowKey);
    }



    public  String getId(int deep, int num) {

        String id = String.valueOf(deep);
        if(num/10 == 0) {
            return id + "_00" + num%10;
        }
        else if(num/100 == 0) {
            return id + "_0" + String.valueOf(num/10%10) + String.valueOf(num%10);
        }
        else return id + "_"+ String.valueOf(num/100) + String.valueOf(num/10%10) + String.valueOf(num%10);
    }

    public String NewId(int deep) {
        numMp.put(deep, numMp.get(deep)+1);
        return getId(deep, numMp.get(deep));
    }

    public void dfs(String id) {
        if(num > MaxNum) return;
        Random random = new Random();
        int NodeNum = random.nextInt(6)+1;
        int deep = Integer.parseInt(id.substring(0, id.indexOf("_"))) + 1;
//        System.out.println(NodeNum);

        for(int i = 1; i <= NodeNum; i++) {
            if(num >= MaxNum) return;
            num++;
            String RowKey = NewId(deep);
            //指定父节点
            con.PutData("dept", RowKey, "base", "fid", id);
            //指定当前节点的名字
            String val = String.valueOf(deep)+"层部门:" + String.valueOf(num);
            con.PutData("dept", RowKey, "base", "name", val);

            if(!id.equals("0_000")) {
                //给父节添加子节点
                con.PutData("dept", id, "subdept", "child_"+String.valueOf(i)+"_id", RowKey);
                con.PutData("dept", id, "subdept", "childNum", String.valueOf(i));
            }
            if(random.nextBoolean())
                dfs(RowKey);
        }
    }
}
