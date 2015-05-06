package edu.gslis.hbase.trec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


/**
 * HBase-based driver that rescores top K documents in 
 * reducer using KDE
 */
public class RunQueryKDEHBase extends RunQueryHBase
{
    
    private static final double[] alphas = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    
    public static class KDEQueryReducer extends RunQueryReducer
    {
        
        Qrels qrels = null;
        Text output = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException 
        {
            String[] queryParams = key.toString().split(",");
            String qid = queryParams[1];
            Iterator<Text> it = values.iterator();
            List<SearchResult> results = new ArrayList<SearchResult>();
            while (it.hasNext()) {
                Text value = it.next();
                String[] fields = value.toString().split("\t");
                SearchResult rs = new SearchResult(fields[0].toString(), Double.valueOf(fields[2]), Long.valueOf(fields[1]));
                results.add(rs);
            }
            // Sort by score
            Collections.sort(results);
            // Only keep the top K
            if (results.size() > TOP)
                results = results.subList(0, TOP);
            
            // Alright, we have our top-K documents
            
            double[] epochs = new double[results.size()];
            double[] scores = new double[results.size()];
            for (int i=0; i<results.size(); i++) {
                epochs[i] = results.get(i).getEpoch();
                scores[i] = results.get(i).getScore();
            }
            
            try {
            
                // 1) Use local R instance to calculate KDE
                RKernelDensity dist = new RKernelDensity("localhost", 6631);
                dist.estimate(epochs, scores);
                
                // 2) Sweep alpha
                for (double alpha: alphas) {
                    
                    // 3) Rescore
                    List<SearchResult> rescored = new ArrayList<SearchResult>();

                    for (SearchResult result: results) {
                        double ll = result.getScore();
                        long epoch = result.getEpoch();
                        double kde = Math.log(dist.density(epoch));
                        
                        double newscore =  alpha*kde + (1-alpha)*ll;

                        SearchResult newres = new SearchResult(result.getDocid(), newscore, epoch);
                        rescored.add(newres);
                    }
                    Collections.sort(rescored);

                    Eval eval = new Eval(results, qrels);
                    double map = eval.map(qid);
                    double p10 = eval.precisionAt(qid, 10);
                    double p20 = eval.precisionAt(qid, 20);
    
                    output.set(map + ","  + "," + p10 + "," + p20);
                    context.write(new Text(key.toString() + "," + alpha),  output);                
                }
            } catch (Exception e) { 
                e.printStackTrace();
            }

        }        
  }
      
  public int run(String[] args) throws Exception 
  {
      String colTableName = args[0];
      String docTableName = args[1];
      String topicFile = args[2];
      String qrelsFile = args[3];
      String outputPath = args[4];

      Configuration config = HBaseConfiguration.create(getConf());
      config.set("colTableName", colTableName);
      Job job = Job.getInstance(config);
      job.setJarByClass(RunQueryKDEHBase.class); 

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
      DistributedCache.addCacheFile(new Path(qrelsFile).toUri(), job.getConfiguration());      
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
      
      boolean b = job.waitForCompletion(true);
      if (!b)
          throw new IOException("error with job!");
      
      return 0;
  }

  public static void main(String[] args) throws Exception {
      
    int res = ToolRunner.run(new Configuration(), new RunQueryKDEHBase(), args);
    System.exit(res);
  }
}