package edu.gslis.hbase.trec;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Calculate collection statistics and store in Hbase
 */
public class RunQueryHBase extends Configured implements Tool 
{
    private static final int TOP = 1000;
    private static final double MU = 1000;

    public static class TrecTableMapper extends TableMapper<Text, Text> 
    {

        HTable statsTable = null; 
        long numDocs = 0;
        long numTerms = 0;
        long numTokens = 0;
        Map<String, Bag> queryMap = new HashMap<String, Bag>();
        
        Text qidKey = new Text();
        Text scoreValue = new Text();
                

        public void map(ImmutableBytesWritable row, Result result, Context context) 
                throws InterruptedException, IOException {

            byte[] bytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("dv"));

            String docid = new String(row.get());
            try {
                
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream objInputStream = new ObjectInputStream(bis);
                Bag dv = (HashBag)objInputStream.readObject();
                
                if (dv.size() > 0) {
                    
                    for (String query: queryMap.keySet()) 
                    {
                        Bag qv = queryMap.get(query);
                                            
                        double score = scoreDirichlet(qv, dv, MU);
                        qidKey.set(query);                    
                        scoreValue.set(docid + "\t" + score);
                        context.write(qidKey, scoreValue);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }        
        }

        public void setup(Context context) throws IOException {
            
            Configuration conf = context.getConfiguration();
            String colTableName = conf.get("colTableName");
            readCollectionStats(colTableName);
            
            Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path localFile: localFiles) {
                System.out.println(localFile.toString());

                if (localFile.getName().contains("topics"))
                    readQueries(localFile);
            }            
        }
        
        /**
         * Side-load the queries
         */
        private void readQueries(Path queryFile) throws IOException
        {
            System.out.println("readQueries: " + queryFile.toString());
            BufferedReader br = new BufferedReader(new FileReader(queryFile.toString()));
            String line = null;
            while ((line = br.readLine()) != null) 
            {
              line = line.toLowerCase();
              String [] fields = line.split(":");
              String [] terms = fields[1].split(" ");   
              Bag bag = new HashBag();
              for (String term: terms)
                  bag.add(term);
              queryMap.put(fields[0], bag);
            }
            br.close();
        }
        
        private void readCollectionStats(String tableName) throws IOException {
            Configuration config = HBaseConfiguration.create();
            statsTable = new HTable(config, tableName);
            Get g = new Get(Bytes.toBytes("#collstats"));
            Result r = statsTable.get(g);
            byte[] bytes = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("cs"));
            String cs = new String(bytes);
            String[] fields = cs.split(",");
            numDocs = Long.parseLong(fields[0]);
            numTerms = Long.parseLong(fields[1]);
            numTokens = Long.parseLong(fields[2]);  
            System.out.println("Read table " + tableName + ": " + numDocs + "," + numTerms + "," + numTokens);
        }
        
        public void cleanup(Context context) throws IOException {
            statsTable.close();
        }
        
        Map<String, Double> collProb = new HashMap<String, Double>();
        
        @SuppressWarnings("unchecked")
        public double scoreDirichlet(Bag qv, Bag dv, double mu) throws IOException {
            double logLikelihood = 0.0;
            Set<String> qterms = qv.uniqueSet();
            for (String q: qterms) {
                double df = dv.getCount(q);
                double dl = dv.size();
                double cp = collPr(q);
                double pr = (df + mu*cp) / (dl + mu);
                double qw = qv.getCount(q);
                logLikelihood += qw * Math.log(pr);
            }
            return logLikelihood;
        }
        
        public double collPr(String term) throws IOException {
            if (collProb.get(term) == null) {
                System.out.println(term);
                Get g = new Get(Bytes.toBytes(term));
                Result r = statsTable.get(g);
                byte[] bytes = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("cf"));
                double cf = 1;
                if (bytes != null) {
                    cf = ByteBuffer.wrap(bytes).getInt();
                }
                double cp = cf/(double)numTokens;
                collProb.put(term,  cp);

            }
            
            return collProb.get(term);
        }
    }
        
    public static class RunQueryReducer extends Reducer<Text, Text, Text, Text> 
    {
        Text output = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException 
        {
            // query \t document \t score
            Iterator<Text> it = values.iterator();
            List<Result> results = new ArrayList<Result>();
            while (it.hasNext()) {
                Text value = it.next();
                String[] fields = value.toString().split("\t");
                Result rs = new Result(fields[0].toString(), Double.valueOf(fields[1]));
                results.add(rs);
            }
            // Sort by score
            Collections.sort(results);
            // Only keep the top K
            if (results.size() > TOP)
                results = results.subList(0, TOP);
            for (Result result: results) {
                output.set(result.getDocid() + "\t" + result.getScore());
                context.write(key, output);
            }
        }
        
        class Result implements Comparable<Result> {
            String docid;
            double score;
            
            public Result(String docid, double score) {
                this.docid = docid;
                this.score = score;
            }

            @Override
            public int compareTo(Result o1) {
                return Double.compare(o1.score, score);
            }
            
            public String getDocid() {
                return docid;
            }
            public double getScore() {
                return score;
            }
        }
    }
      
  public int run(String[] args) throws Exception 
  {
      String colTableName = args[0];
      String docTableName = args[1];
      String topicFile = args[2];
      String outputPath = args[3];

      Configuration config = HBaseConfiguration.create();
      config.set("colTableName", colTableName);
      Job job = Job.getInstance(config);
      job.setJarByClass(RunQueryHBase.class); 

      Scan scan = new Scan();
      scan.setCaching(500);     
      scan.setCacheBlocks(false);

      TableMapReduceUtil.initTableMapperJob(
          docTableName, 
          scan, 
          TrecTableMapper.class,
          Text.class, 
          Text.class, 
          job
      );
      

      job.setReducerClass(RunQueryReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      DistributedCache.addCacheFile(new Path(topicFile).toUri(), job.getConfiguration());      
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
      
      boolean b = job.waitForCompletion(true);
      if (!b)
          throw new IOException("error with job!");
      
      return 0;
  }

  public static void main(String[] args) throws Exception {
      
    int res = ToolRunner.run(new Configuration(), new RunQueryHBase(), args);
    System.exit(res);
  }
}