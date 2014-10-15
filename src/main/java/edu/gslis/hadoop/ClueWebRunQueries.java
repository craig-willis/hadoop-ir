package edu.gslis.hadoop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cmu.lemurproject.WarcRecord;
import edu.cmu.lemurproject.WritableWarcRecord;

/**
 * MapReduce job to run queries against the ClueWeb09 collection
 */
public class ClueWebRunQueries extends Configured implements Tool 
{
    
    private static final String TOKENIZER = "[^0-9A-Za-z]+";
    private static final double MU = 2500;
    private static final int TOP = 1000;
    private static final double TOKENS = 46413966793D;

    public static class CWRunQueriesMapper extends Mapper<LongWritable, WritableWarcRecord, Text, Text>
    {
        private Map<String, String[]> queryMap = new HashMap<String, String[]>();
        private Map<String, Double> collFreqMap = new HashMap<String, Double>();
        
        static Connection conn = null;
        static PreparedStatement statement = null;
        
        public void setup(Context context) throws IOException {
            System.out.println("setup");
            Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path localFile: localFiles) {
                System.out.println(localFile.toString());

                if (localFile.getName().contains("topics"))
                    readQueries(localFile);
            }
            
            // Loading the complete set of stats proved problematic in Hadoop. Instead,
            // use a pre-loaded H2 database running on gchead.
            try {
                Class.forName("org.h2.Driver");
                conn = DriverManager.getConnection("jdbc:h2:tcp://gchead/./clueweb09stats");
                statement = conn.prepareStatement("select tf from term_stats where term = ?");
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);

            }
        }
        
        /**
         * Executes query against H2 database
         * @param term
         * @return
         * @throws IOException
         */
        public double getTermFreq(String term) throws IOException {
            double tf = 1;
            
            try {
                if (collFreqMap.get(term) == null) {
                    statement.setString(1,  term);
                    ResultSet rs = statement.executeQuery();
                    if (rs.next())
                        tf = rs.getInt(1);
                    rs.close(); 
                    collFreqMap.put(term, tf);
                }
                else
                    tf = collFreqMap.get(term);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException(e.getMessage(), e);
            }
            return tf;
        }
        
        public void cleanup(Context context) throws IOException {
            try {
                statement.close();
                conn.close();
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        /**
         * Side-load the queries
         */
        public void readQueries(Path queryFile) throws IOException
        {
            System.out.println("readQueries: " + queryFile.toString());
            BufferedReader br = new BufferedReader(new FileReader(queryFile.toString()));
            String line = null;
            while ((line = br.readLine()) != null) 
            {
              line = line.toLowerCase();
              String [] fields = line.split(":");
              String [] terms = fields[1].split(" ");              
              queryMap.put(fields[0], terms);
            }
            br.close();
        }
        
        /**
         * LM scoring with Dirichlet smoothing
         */
        public double scoreDirichlet(String[] qterms, java.util.Map<String, Integer> docTF, Long docLength, Double mu)
            throws IOException
        {
            double logLikelihood = 0.0d;
            
            Bag terms = new HashBag();
            for (String qterm: qterms)
                terms.add(qterm);
                
            
            for (Object t: terms.uniqueSet()) {
                String term = (String)t;
                
                int qtf = terms.getCount(t);
                
                int docFreq = 0;
                if (docTF.get(term) != null)
                    docFreq = docTF.get(term);
                
                double tf = getTermFreq(term);
                
                double collProb = tf / TOKENS;
                
                double pr = (docFreq + mu*collProb) / (docLength + mu);
                
                logLikelihood += qtf * Math.log(pr);

            }
            return logLikelihood;            
        }

        Text qidKey = new Text();
        Text scoreValue = new Text();
        
        private final static Pattern
            headerPat = Pattern.compile("^(.*?)<",  
                Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
            scriptPat = Pattern.compile("(?s)<script(.*?)</script>", 
                Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
            tagsPat = Pattern.compile("<[^>]+>", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
            spacePat = Pattern.compile("[ \n\r\t]+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
        
        private String getText(WarcRecord record) {
            String webpage = record.getContentUTF8();
            webpage = headerPat.matcher(webpage).replaceFirst("<");
            webpage = scriptPat.matcher(webpage).replaceAll(" ");
            webpage = tagsPat.matcher(webpage).replaceAll(" ");
            webpage = spacePat.matcher(webpage).replaceAll(" ");
            
            return webpage;            
        }
        /**
         * Reads each WARC recrod and scores against all queries
         * @param key   Document identifier
         * @param value WARC record
         */
        public void map(LongWritable key, WritableWarcRecord value, Context context) throws IOException, InterruptedException 
        {            
            java.util.Map<String, Integer> docTF = new HashMap<String, Integer>();
            
            // Convert from HTML to text
            WarcRecord record = value.getRecord();
            String docid = record.getHeaderMetadataItem("WARC-TREC-ID");
            System.out.println("Scoring " + docid);
            String text = getText(record);
            long docLen = 0;
            Scanner scan = new Scanner(text.toLowerCase()).useDelimiter(TOKENIZER);
            while (scan.hasNext()) {
                docLen++;
                String term = scan.next();
                
                int tf = 0;
                if (docTF.get(term) != null)
                    tf = docTF.get(term);
                
                docTF.put(term, ++tf);
            }

            // for each query, score the document
            if (docLen > 0) {
                
                for (String query: queryMap.keySet()) 
                {
                    String[] qterms = queryMap.get(query);
                                        
                    double score = scoreDirichlet(qterms, docTF, docLen, MU);
                    qidKey.set(query);                    
                    scoreValue.set(docid + "\t" + score);
                    context.write(qidKey, scoreValue);
                }
            }
        }
    }
    


    public static class CWRunQueriesReducer extends Reducer<Text, Text, Text, Text> 
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
        
        String collectionPath = args[0];
        String topicFile = args[1];
        String outputPath = args[2];

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "clueweb09-run-queries");
        job.setJarByClass(ClueWebRunQueries.class);
        job.setMapperClass(CWRunQueriesMapper.class);
        job.setReducerClass(CWRunQueriesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        DistributedCache.addCacheFile(new Path(topicFile).toUri(), job.getConfiguration());
        
        // Input path is path to filtered ClueWeb data
        FileInputFormat.addInputPath(job, new Path(collectionPath));  
        // Path to write results
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
                       
        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new ClueWebRunQueries(), args);
       
       System.exit(res);
    }
}