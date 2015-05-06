package edu.gslis.hbase.trec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class DumpIndexHBase {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String cmd = args[0];
        String colTableName = args[1];
        String docsTableName = args[2];
        if (cmd.equals("s")) 
        {
           
            Configuration config = HBaseConfiguration.create();
            HTable table = new HTable(config, colTableName);
            
            Get g = new Get(Bytes.toBytes("#collstats"));
            Result r = table.get(g);
            byte[] bytes = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("cs"));
            String cs = new String(bytes);
            String[] fields = cs.split(",");
            long numDocs = Long.parseLong(fields[0]);
            long numTerms = Long.parseLong(fields[1]);
            long numTokens = Long.parseLong(fields[2]);  
            
            System.out.println(
                "documents: " + numDocs + "\n" + 
                "terms: " + numTerms + "\n" + 
                "tokens: " + numTokens
            );
            table.close();

        }
        else if (cmd.equals("dv")) {
            String docno = args[3];
            Configuration config = HBaseConfiguration.create();
            HTable table = new HTable(config, docsTableName);
            
            Get g = new Get(Bytes.toBytes(docno));
            Result r = table.get(g);
            byte[] value = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("dv"));
            
            
            ByteArrayInputStream bis = new ByteArrayInputStream(value);
            ObjectInputStream objInputStream = new ObjectInputStream(bis);
            FeatureVector dv = (FeatureVector)objInputStream.readObject();
            Set<String> terms = dv.getFeatures();
            for (String term: terms) {
                double count = dv.getFeatureWeight(term);
                System.out.println(term + " " + count);
            }
            table.close();
        }
    }
}
