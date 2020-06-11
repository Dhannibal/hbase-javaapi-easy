package org;

import org.apache.log4j.BasicConfigurator;
import org.cuit.hbase.experiment.experiment3;
import org.junit.Test;

import java.util.List;

public class Htest {
    static  experiment3 ex = new experiment3();
    @Test
    public void test() {
        BasicConfigurator.configure();
        String ss = "00_11";
        System.out.println(ss.substring(0, ss.indexOf("_")));
    }
    @Test
    public void testCreate()  {
        try{
            ex.createDept();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testShowDeep() {
        try {
            ex.showDeep(3);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testScanByRowKey() {
        try{
            List<String> ls =  ex.scanByRowKey("1_001");
            for(String ss : ls) {
                System.out.println(ss);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testAddDep() {
        try {
            System.out.println(ex.addDep("1_001", "AAA"));
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testDeleteDep() {
        try {
            ex.deleteDep("1_001", "1_002");
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
}
