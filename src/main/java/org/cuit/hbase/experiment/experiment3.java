package org.cuit.hbase.experiment;

import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class experiment3 {
    private int num;
    private static final int MaxNum = 30;
    private final HbaseConnection con;
    private final HashMap<Integer, Integer>numMp;

    public experiment3() {
        con = new HbaseConnection();
        numMp = new HashMap<>();
        for(int i = 0; i <= MaxNum; i++) {
            numMp.put(i, 0);
        }
    }

    public void createDept() throws Exception {
        if(con.tableExists("dept")) {
            con.DeleteTableByName("dept");
        }

        con.createTable("dept", "base", "subdept");

        num = 0;
        dfs("0_000");
    }

    public void showDeep(int deep) throws IOException {
        con.scanDeep("dept", deep);
    }

    //获得子部门id
    public List<String> scanByRowKey(String rowKey) throws IOException {
        return con.scanByRowKey("dept", rowKey);
    }

    public String addDep(String faRowKey, String name) throws IOException {
        int cnt = con.getRowKeyChildNum("dept", faRowKey);
        int deep = Integer.parseInt(faRowKey.substring(0, faRowKey.indexOf("_"))) + 1;
        Random random = new Random();
        String chRowKey = deep+"_" + random.nextInt(9999)+9721;
        //在1_001节点上加上一个新的节点
        PutData(faRowKey, "subdept", "child_"+String.valueOf(cnt+1) +"_id", chRowKey);
        PutData(faRowKey, "subdept", "childNum", String.valueOf(cnt+1));
        PutData(chRowKey, "base", "name", name);
        PutData(chRowKey, "base", "fid", faRowKey);
        return chRowKey;
    }

    public void deleteDep(String toRowKey, String nowRowKey) throws IOException {
        List<String> childList = con.scanByRowKey("dept", nowRowKey);
        int cnt = con.getRowKeyChildNum("dept", toRowKey);
        if(childList == null) return;
        for(String id : childList) {
            cnt++;
            PutData(toRowKey, "subdept", "child_"+String.valueOf(cnt)+"_id", id);
            PutData(id, "base", "fid", toRowKey);
        }
        //维护子部门数
        PutData(toRowKey, "subdept", "childNum", String.valueOf(cnt));
        con.deleteRowKey("dept", nowRowKey);
    }

    public void PutData(String rowKey, String family, String data, String info) {
        Put put = new Put(rowKey.getBytes()); //指定rowKey
        put.addColumn(family.getBytes(), data.getBytes(), info.getBytes());
        try {
            con.AddInfo("dept", put);
        } catch (IOException e) {
            e.printStackTrace();
        }

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

        Random random = new Random();
        int NodeNum = random.nextInt(6)+1;
        int deep = Integer.parseInt(id.substring(0, id.indexOf("_"))) + 1;
        System.out.println(NodeNum);

        for(int i = 1; i <= NodeNum; i++) {
            String RowKey = NewId(deep);
            num++;
            if(num >= MaxNum) return;
            //指定父节点
            PutData(RowKey, "base", "fid", id);
            //指定当前节点的名字
            String val = String.valueOf(deep)+"层部门:" + String.valueOf(num);
            PutData(RowKey, "base", "name", val);

            if(!id.equals("0_000")) {
                //给父节添加子节点
                PutData(id, "subdept", "child_"+String.valueOf(i)+"_id", RowKey);
                PutData(id, "subdept", "childNum", String.valueOf(i));
            }
            if(random.nextBoolean())
                dfs(RowKey);
        }
    }
}
