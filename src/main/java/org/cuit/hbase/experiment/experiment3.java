package org.cuit.hbase.experiment;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

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
                Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("base"));
                System.out.print(rowKey+"   ");
                for(Map.Entry<byte[], byte[]> entry:familyMap.entrySet()) {
                    System.out.println(Bytes.toString(entry.getKey()) + ":" + Bytes.toString(entry.getValue())+"  ");
                }
                System.out.println();
            }
        }
    }

    //根据rowKey查询子部门
    public List<String> scanByRowKeyForChild(String rowKey) throws IOException {
        List<String> childIdList = new ArrayList<String>();
        System.out.println("查询"+ rowKey+"的子部门信息");
        Map<byte[], byte[]> familyMap = scanByRowKeyForChildMap(rowKey);
        for(Map.Entry<byte[], byte[]> entry:familyMap.entrySet()){
            System.out.println(Bytes.toString(entry.getValue()));
            childIdList.add(Bytes.toString(entry.getValue()));
        }
        return childIdList;
    }

    public Map<byte[], byte[]> scanByRowKeyForChildMap(String rowKey) throws IOException {
        return con.getFamily("dept", rowKey, "subdept");
    }

    //根据rowKey查询基本信息
    public void scanByRowKeyForBasic(String rowKey) throws IOException {
        System.out.println("查询"+ rowKey+"的基本信息");
        Map<byte[], byte[]> familyMap = scanByRowKeyForBasicMap(rowKey);
        for(Map.Entry<byte[], byte[]> entry:familyMap.entrySet()){
            System.out.println(Bytes.toString(entry.getKey()) + ":" + Bytes.toString(entry.getValue())+"  ");
        }
    }

    public Map<byte[], byte[]> scanByRowKeyForBasicMap(String rowKey) throws IOException {
        return con.getFamily("dept", rowKey, "base");
    }

    public int getRowKeyChildNum( String rowKey) throws IOException {
        return con.getFamilyCount("dept", rowKey, "subdept");
    }

    public String addDep(String faRowKey, String name) throws IOException {
        int cnt = getRowKeyChildNum(faRowKey);
        int deep = Integer.parseInt(faRowKey.substring(0, faRowKey.indexOf("_"))) + 1;
        Random random = new Random();
        String chRowKey = deep+"_" + random.nextInt(9999)+9721;
        //在1_001节点上加上一个新的节点
        con.PutData("dept", faRowKey, "subdept", "child_"+String.valueOf(cnt+1) +"_id", chRowKey);
        con.PutData("dept", chRowKey, "base", "name", name);
        con.PutData("dept", chRowKey, "base", "fid", faRowKey);
        return chRowKey;
    }

    public void deleteDep(String toRowKey, String nowRowKey) throws IOException {
        Map<byte[], byte[]> familyMap = scanByRowKeyForBasicMap(nowRowKey);
        for(Map.Entry<byte[], byte[]> entry:familyMap.entrySet()) {
            if(Bytes.toString(entry.getKey()).equals("fid")) {
                String fid = Bytes.toString(entry.getValue());
                Map<byte[], byte[]> fidMp =  con.getFamily("dept", fid, "subdept");
                for(Map.Entry<byte[], byte[]> mapEntry:fidMp.entrySet()) {
                    if(Bytes.toString(mapEntry.getValue()).equals(nowRowKey))
                        con.deleteByRowKeyCell("dept", fid, "subdept", Bytes.toString(mapEntry.getKey()));
                }
            }
        }

        List<String> childList = scanByRowKeyForChild(nowRowKey);
        int cnt = getRowKeyChildNum(toRowKey);
        if(childList == null) return;
        for(String id : childList) {
            cnt++;
            con.PutData("dept", toRowKey, "subdept", "child_"+String.valueOf(cnt)+"_id", id);
            con.PutData("dept", id, "base", "fid", toRowKey);
        }
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
            //指定当前节点的名字
            String val = String.valueOf(deep)+"层部门" + String.valueOf(num)+"号";
            con.PutData("dept", RowKey, "base", "name", val);

            if(!id.equals("0_000")) {
                //指定父节点
                con.PutData("dept", RowKey, "base", "fid", id);
                //给父节添加子节点
                con.PutData("dept", id, "subdept", "child_"+String.valueOf(i)+"_id", RowKey);
            }
            if(random.nextBoolean())
                dfs(RowKey);
        }
    }
}
