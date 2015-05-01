package edu.gslis.hbase.trec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Set;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Calculate collection statistics and store in Hbase
 */
public class CollectionStatsHBase extends Configured implements Tool 
{
    static enum CollectionStats {
        numberOfDocuments,
        numberOfTerms,
        numberOfTokens
    }
    
    public static class TrecTableMapper extends TableMapper<Text, IntWritable> {

        Text term = new Text();
        IntWritable freq = new IntWritable();
        public void map(ImmutableBytesWritable row, Result result, Context context) 
                throws InterruptedException, IOException {

            context.getCounter(CollectionStats.numberOfDocuments).increment(1L);

            byte[] bytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("dv"));
            
            try {
                
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream objInputStream = new ObjectInputStream(bis);
                Bag docVector = (HashBag)objInputStream.readObject();
                Set<String> terms = (Set<String>)docVector.uniqueSet();
                for (String t: terms) {
                    term.set(t);
                    int count = docVector.getCount(t);
                    freq.set(docVector.getCount(t));
                    context.write(term, freq);
                    
                    context.getCounter(CollectionStats.numberOfTokens).increment(count);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }        
        }
    }
        
    
    public static class CollStatsReducer extends TableReducer <Text, IntWritable, ImmutableBytesWritable> 
    {       

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException 
        {
            context.getCounter(CollectionStats.numberOfTerms).increment(1L);

            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();
                
            String term = key.toString();

            Put put = new Put(Bytes.toBytes(term));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("cf"), Bytes.toBytes(sum));
            context.write(null, put);
        }
    }
      
  public int run(String[] args) throws Exception 
  {
      Configuration config = HBaseConfiguration.create();
      Job job = Job.getInstance(config);
      job.setJarByClass(CollectionStatsHBase.class); 

      Scan scan = new Scan();
      scan.setCaching(500);     
      scan.setCacheBlocks(false);

      TableMapReduceUtil.initTableMapperJob(
              "test",        // input HBase table name
              scan,             // Scan instance to control CF and attribute selection
              TrecTableMapper.class,   // mapper
              Text.class,             // mapper output key
              IntWritable.class,             // mapper output value
              job);
      
      TableMapReduceUtil.initTableReducerJob(
              "stats",        // output table
              CollStatsReducer.class,    // reducer class
              job);
      job.setNumReduceTasks(1);   // at least one, adjust as required


      
      boolean b = job.waitForCompletion(true);
      if (!b) {
          throw new IOException("error with job!");
      }
      
      
    long numDocs = job.getCounters().findCounter(CollectionStats.numberOfDocuments).getValue();
    long numTerms = job.getCounters().findCounter(CollectionStats.numberOfTerms).getValue();
    long numTokens = job.getCounters().findCounter(CollectionStats.numberOfTokens).getValue();
    HTable statsTable = new HTable(config, "stats");
    Put put = new Put(Bytes.toBytes("#collstats"));
    put.add(Bytes.toBytes("cf"), Bytes.toBytes("cs"), Bytes.toBytes(numDocs + "," + numTerms + "," + numTokens));
    statsTable.put(put);
    statsTable.close();
      return 0;
  }

  public static void main(String[] args) throws Exception {
      
    int res = ToolRunner.run(new Configuration(), new CollectionStatsHBase(), args);
    System.exit(res);
  }
}