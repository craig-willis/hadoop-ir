package edu.gslis.hbase.trec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "test");
        Get g = new Get(Bytes.toBytes("FT911-99"));
        Result r = table.get(g);
        byte[] value = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("dv"));
        
        
        ByteArrayInputStream bis = new ByteArrayInputStream(value);
        ObjectInputStream objInputStream = new ObjectInputStream(bis);
        Bag bag = (HashBag)objInputStream.readObject();

        
        table.close();
    }
}
