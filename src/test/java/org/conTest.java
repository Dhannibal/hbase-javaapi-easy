package org;

import org.cuit.hbase.experiment.HbaseConnection;
import org.junit.Test;

public class conTest {
    static HbaseConnection con = new HbaseConnection();

    @Test
    public void testShowAllTables() {
        try {
            con.ShowAllTables();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testTableExists() {
        try {
            System.out.println(con.tableExists("test"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createTable() {
        try {
            con.createTable("test", "test");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDeleteTable() {
        try {
            con.DeleteTableByName("test");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testNamespaceExist() {
        try {
            System.out.println(con.NamespaceExist("test"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateNamespace() {
        try {
            System.out.println(con.createNamespace("test"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDeleteNamespace() {
        try {
            System.out.println(con.DeleteNamespace("test"));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
