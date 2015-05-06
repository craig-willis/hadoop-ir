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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    protected static final int TOP = 1000;
    private static final double[] mus = {100, 300, 500, 700, 1000, 1500, 2000, 2500};
    private static final double[] lambdas = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    
    public static class TrecTableMapper extends TableMapper<Text, Text> 
    {

        HTable statsTable = null; 
        long numDocs = 0;
        long numTerms = 0;
        long numTokens = 0;
        Map<String, FeatureVector> queryMap = new HashMap<String, FeatureVector>();
        
        Text qidKey = new Text();
        Text scoreValue = new Text();
                

        public void map(ImmutableBytesWritable row, Result result, Context context) 
                throws InterruptedException, IOException {

            byte[] bytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("dv"));
            
            long epoch = Bytes.toLong(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("epoch")));

            String docid = new String(row.get());
            try {
                
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream objInputStream = new ObjectInputStream(bis);
                FeatureVector dv = (FeatureVector)objInputStream.readObject();
                
                if (dv.getLength() > 0) {
                    
                    for (String query: queryMap.keySet()) 
                    {
                        FeatureVector qv = queryMap.get(query);
                                            
                        for (double mu: mus) {
                            double score = scoreDirichlet(qv, dv, mu);
                            qidKey.set("dir," + query + "," + mu);                    
                            scoreValue.set(docid + "\t" + epoch + "\t" + score);
                            context.write(qidKey, scoreValue);
                        }
                        
                        /*
                        for (double mu: mus) {
                            double score = scoreCrossEntropy(qv, dv, mu);
                            qidKey.set("cer," + query + "," + mu);                    
                            scoreValue.set(docid + "\t" + epoch + "\t" + score);
                            context.write(qidKey, scoreValue);
                        }
                        
                        for (double lambda: lambdas) {
                            double score = scoreJM(qv, dv, lambda);
                            qidKey.set("jm," + query + "," + lambda);                    
                            scoreValue.set(docid + "\t" + epoch + "\t" + score);
                            context.write(qidKey, scoreValue);
                        }
                        
                        for (double lambda: lambdas) {
                            for (double mu: mus) {
                                double score = scoreTwoStage(qv, dv, mu, lambda);
                                qidKey.set("twostage," + query + "," + mu + ":" + lambda);                    
                                scoreValue.set(docid + "\t" + epoch + "\t" + score);
                                context.write(qidKey, scoreValue);
                            }
                        }
                        */
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
              //51,5:5:0.1      airbus:0.40790284157041523 subsidy:0.27441106804306636 aircraft:0.11027637707219344 trade:0.10518909354136353 germany:0.10222061977296132
              // 51      airbus:1 subsidy:1
              String [] fields = line.split("\t");
              String queryParams = fields[0];              
              FeatureVector qv = new FeatureVector();
              String[] termWeights = fields[1].split(" ");
              for (String termWeight: termWeights) {
                  String[] tw = termWeight.split(":");
                  String term = tw[0];
                  double weight = Double.valueOf(tw[1]);
                  qv.addTerm(term, weight);
              }
              queryMap.put(queryParams, qv);
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
        
        public double scoreDirichlet(FeatureVector qv, FeatureVector dv, double mu) throws IOException {
            double logLikelihood = 0.0;
            Set<String> qterms = qv.getFeatures();
            for (String q: qterms) {
                double df = dv.getFeatureWeight(q);
                double dl = dv.getLength();
                double cp = collPr(q);
                double pr = (df + mu*cp) / (dl + mu);
                double qw = qv.getFeatureWeight(q);
                logLikelihood += qw * Math.log(pr);
            }
            return logLikelihood;
        }
        
        public double scoreJM(FeatureVector qv, FeatureVector dv, double lambda) throws IOException {
            double logLikelihood = 0.0;
            Set<String> qterms = qv.getFeatures();
            for (String q: qterms) {
                double df = dv.getFeatureWeight(q);
                double dl = dv.getLength();
                double cp = collPr(q);
                double dp = df/dl;
                double pr = (1-lambda)*dp + lambda*cp;
                double qw = qv.getFeatureWeight(q);
                logLikelihood += qw * Math.log(pr);
            }
            return logLikelihood;
        }

        public double scoreTwoStage(FeatureVector qv, FeatureVector dv, double mu, double lambda) throws IOException {
            double logLikelihood = 0.0;
            Set<String> qterms = qv.getFeatures();
            for (String q: qterms) {
                double df = dv.getFeatureWeight(q);
                double dl = dv.getLength();
                double cp = collPr(q);
                double pr = (1-lambda)*( (df +  mu*cp) / (dl + mu)) + lambda*cp;
                double qw = qv.getFeatureWeight(q);
                logLikelihood += qw * Math.log(pr);
            }
            return logLikelihood;
        }
        
        public double scoreCrossEntropy(FeatureVector qv, FeatureVector dv, double mu) throws IOException {
            double logLikelihood = 0.0;
            Set<String> qterms = qv.getFeatures();
            for (String q: qterms) {
                double df = dv.getFeatureWeight(q);
                double dl = dv.getLength();
                double cp = collPr(q);
                
                double qf = qv.getFeatureWeight(q);
                double ql = qv.getLength();
                
                double qp = qf/ql;
                //double dp = df/dl;
                double dp = (df + mu*cp) / (dl + mu);


                logLikelihood += qp * Math.log(dp/cp);
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
        Qrels qrels = null;
        Text output = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException 
        {
            //            "twostage," + query + "," + mu + ":" + lambda
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
            
            Eval eval = new Eval(results, qrels);
            double map = eval.map(qid);
            double p10 = eval.precisionAt(qid, 10);
            double p20 = eval.precisionAt(qid, 20);

            output.set(map + ","  + "," + p10 + "," + p20);
            context.write(key,  output);
            /*
            for (SearchResult result: results) {
                output.set(result.getDocid() + "\t" + result.getScore());
                context.write(key, output);
            }
            */
            
            //context.write(key,  new Text("metrics," + map + ","  + "," + p10 + "," + p20));

        }
        
        public void setup(Context context) throws IOException {
                        
            Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path localFile: localFiles) {
                System.out.println(localFile.toString());
                if (localFile.getName().contains("qrels"))
                    readQrels(localFile);
            }            
        }
        
        public void readQrels(Path qrelsFile) {

            qrels = new Qrels(qrelsFile.toString(), false, 1);
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
      DistributedCache.addCacheFile(new Path(qrelsFile).toUri(), job.getConfiguration());      
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