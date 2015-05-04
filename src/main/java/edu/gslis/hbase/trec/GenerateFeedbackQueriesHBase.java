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
import java.util.HashSet;
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

import edu.gslis.textrepresentation.FeatureVector;
import edu.gslis.utils.KeyValuePair;
import edu.gslis.utils.ScorableComparator;


/**
 * Generate feedback queries using Lavrenko's RM
 */
public class GenerateFeedbackQueriesHBase extends Configured implements Tool 
{
    private static final int TOP = 1000;
    //private static final double[] mus = {100, 300, 500, 700, 1000, 1500, 2000, 2500};
    //private static final double[] lambdas = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    //private static final int[] numDocs = {5, 10, 15, 20, 25, 30, 35, 40, 45, 50};
    //private static final int[] numTerms = {5, 10, 15, 20, 25, 30, 35, 40, 45, 50};
    //private static final double[] rmLambdas = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    
    private static final double[] mus = {1500};
    private static final int[] numDocs = {5, 10, 15, 20, 25, 30, 35, 40, 45, 50};
    private static final int[] numTerms = {5, 10, 15, 20, 25, 30, 35, 40, 45, 50};
    private static final double[] rmLambdas = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    
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
                            scoreValue.set(docid + "\t" + score);
                            context.write(qidKey, scoreValue);
                        }
                        
                        /*
                        for (double mu: mus) {
                            double score = scoreCrossEntropy(qv, dv, mu);
                            qidKey.set("cer," + query + "," + mu);                    
                            scoreValue.set(docid + "\t" + score);
                            context.write(qidKey, scoreValue);
                        }
                        */
                        
                        /*
                        for (double lambda: lambdas) {
                            double score = scoreJM(qv, dv, lambda);
                            qidKey.set("jm," + query + "," + lambda);                    
                            scoreValue.set(docid + "\t" + score);
                            context.write(qidKey, scoreValue);
                        }
                        
                        for (double lambda: lambdas) {
                            for (double mu: mus) {
                                double score = scoreTwoStage(qv, dv, mu, lambda);
                                qidKey.set("twostage," + query + "," + mu + ":" + lambda);                    
                                scoreValue.set(docid + "\t" + score);
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
            /*
          FeatureVector qv = new FeatureVector(null);
          qv.addTerm("airbus");
          qv.addTerm("subsidy");
          queryMap.put("51", qv);
          */
            System.out.println("readQueries: " + queryFile.toString());
            BufferedReader br = new BufferedReader(new FileReader(queryFile.toString()));
            String line = null;
            while ((line = br.readLine()) != null) 
            {
              line = line.toLowerCase();
              String [] fields = line.split(":");
              String [] terms = fields[1].split(" ");   
              FeatureVector qv = new FeatureVector(null);
              for (String term: terms)
                  qv.addTerm(term.trim());
              queryMap.put(fields[0], qv);
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
                double cp = (1+cf)/(double)numTokens;
                collProb.put(term,  cp);

            }
            
            return collProb.get(term);
        }
    }
        
    public static class RunQueryReducer extends Reducer<Text, Text, Text, Text> 
    {
        HTable docsTable = null; 
        Map<String, FeatureVector> queryMap = new HashMap<String, FeatureVector>();

        Text output = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException 
        {
            // query \t document \t score
            String[] queryFields = key.toString().split(",");
            String qid = queryFields[1];
            FeatureVector qv = queryMap.get(qid);
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
            
            // Generate the RM3 queries
            for (int fbDocs: numDocs) 
            {
                FeatureVector fv = getFeedbackVector(fbDocs, results);
                
                for (int fbTerms: numTerms) 
                {
                    FeatureVector tmpfv = copyFeatureVector(fv);
                    tmpfv.clip(fbTerms);
                    tmpfv.normalize();
                    for (double rmLambda: rmLambdas) 
                    {
                        FeatureVector rm3 =
                                FeatureVector.interpolate(qv, tmpfv, rmLambda);
                        rm3.normalize();    

                        String params = fbDocs + ":" + fbTerms + ":" + rmLambda;

                        context.write(new Text(qid + "," + params), new Text(toString(rm3)));
                    }                
                }
            }
        }
        
                
        public String toString(FeatureVector fv) 
        {
            List<KeyValuePair> kvpList = new ArrayList<KeyValuePair>(fv.getFeatureCount());
            Set<String> terms = fv.getFeatures();
            for (String term: terms) {
                double weight = fv.getFeatureWeight(term);
                KeyValuePair keyValuePair = new KeyValuePair(term, weight);
                kvpList.add(keyValuePair);
            }
            ScorableComparator comparator = new ScorableComparator(true);
            Collections.sort(kvpList, comparator);


            StringBuffer sb = new StringBuffer();
            
            Iterator<KeyValuePair> it = kvpList.iterator();
            while(it.hasNext()) {
                KeyValuePair pair = it.next();
                sb.append(" " + pair.getKey() + ":" + pair.getScore());
            }
            return sb.toString().trim();            
        }
        
        public FeatureVector copyFeatureVector(FeatureVector fv) {
            FeatureVector copy = new FeatureVector(null);
            Iterator<String> it = fv.iterator();
            while(it.hasNext()) {
                String term = it.next();
                copy.addTerm(term, fv.getFeatureWeight(term));
            }
            return copy;
        }
        
        public static FeatureVector cleanModel(FeatureVector model) {
            FeatureVector cleaned = new FeatureVector(null);
            Iterator<String> it = model.iterator();
            while(it.hasNext()) {
                String term = it.next();
                if(term.length() < 3 || term.matches(".*[0-9].*"))
                    continue;
                cleaned.addTerm(term, model.getFeatureWeight(term));
            }
            cleaned.normalize();
            return cleaned;
        }
        
        public void setup(Context context) throws IOException {
            
            Configuration conf = context.getConfiguration();
            String tableName = conf.get("docTableName");

            Configuration config = HBaseConfiguration.create();
            docsTable = new HTable(config, tableName);
            
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
            /*
            FeatureVector qv = new FeatureVector(null);
            qv.addTerm("airbus");
            qv.addTerm("subsidy");
            queryMap.put("51", qv);
            */
            System.out.println("readQueries: " + queryFile.toString());
            BufferedReader br = new BufferedReader(new FileReader(queryFile.toString()));
            String line = null;
            while ((line = br.readLine()) != null) 
            {
              line = line.toLowerCase();
              String [] fields = line.split(":");
              String [] terms = fields[1].split(" ");   
              FeatureVector qv = new FeatureVector(null);
              for (String term: terms)
                  qv.addTerm(term.trim());
              queryMap.put(fields[0], qv);
            }
            br.close();
        }
        
        public void cleanup(Context context) throws IOException {
            docsTable.close();
        }
        
        public FeatureVector getDocVector(String docno) throws IOException, ClassNotFoundException {
            Get g = new Get(Bytes.toBytes(docno));
            org.apache.hadoop.hbase.client.Result r = docsTable.get(g);
            byte[] bytes = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("dv"));
            
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream objInputStream = new ObjectInputStream(bis);
            FeatureVector dv = (FeatureVector)objInputStream.readObject();
            return dv;
        }
        
        public FeatureVector getFeedbackVector(int fbDocs, List<Result> results) 
        {
            Set<String> vocab = new HashSet<String>();
            List<FeatureVector> fbDocVectors = new LinkedList<FeatureVector>();

            FeatureVector rm = new FeatureVector(null);

            if (fbDocs > results.size())
                fbDocs = results.size();

            double[] rsvs = new double[fbDocs];
            
            try 
            {
                for (int k=0; k < fbDocs; k++) {
                    Result res = results.get(k);
                    double score = res.getScore();
                    rsvs[k] = Math.exp(score);
    
                    FeatureVector dv = getDocVector(res.getDocid());
    
                    vocab.addAll(dv.getFeatures());
                    fbDocVectors.add(dv);
                }
    
                
                Iterator<String> it = vocab.iterator();
                while(it.hasNext()) {
                    String term = it.next();
                    
                    double fbWeight = 0;
                    for (int i=0; i<fbDocVectors.size(); i++) {                    
                        FeatureVector docVector = fbDocVectors.get(i);
                        double rsv = rsvs[i];
                        
                        double docProb = docVector.getFeatureWeight(term) / docVector.getLength();
                        double docWeight = 1.0;                        
                        docProb *= rsv;
                        docProb *= docWeight;
                        fbWeight += docProb;                            
                    }
                    
                    fbWeight /= fbDocVectors.size();
                    
                    rm.addTerm(term, fbWeight);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return cleanModel(rm);
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

      Configuration config = HBaseConfiguration.create(getConf());
      config.set("colTableName", colTableName);
      config.set("docTableName", docTableName);
      Job job = Job.getInstance(config);
      job.setJarByClass(GenerateFeedbackQueriesHBase.class); 

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
      
    int res = ToolRunner.run(new Configuration(), new GenerateFeedbackQueriesHBase(), args);
    System.exit(res);
  }
}