package edu.gslis.hadoop.trec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;

/**
 * MapReduce job to run queries against the ClueWeb09 collection
 */
public class TrecQueryStats extends Configured implements Tool 
{
    private static final String TOKENIZER = "[^0-9A-Za-z]+";


    public static class TrecQueryStatsMapper extends Mapper<LongWritable, TrecDocument, Text, IntWritable>
    {
        // Set of unique query terms
        private Set<String> queryTerms = new TreeSet<String>();   
        // Set of stopwords
        private Set<String> stoplist = new TreeSet<String>();
        
        IntWritable freq = new IntWritable();
        Text termPair = new Text();

        
        /**
         * Side load the topics file and stopwords list
         */
        public void setup(Context context) throws IOException {
            System.out.println("setup");
            Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path localFile: localFiles) {
                System.out.println(localFile.toString());

                if (localFile.getName().contains("stoplist"))
                    readStoplist(localFile);

                if (localFile.getName().contains("topics"))
                    readQueries(localFile);
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
              String [] terms = line.split(" ");        
              for (String term: terms) {
                  queryTerms.add(term);
              }
            }
            br.close();
        }
        
        /**
         * Side-load the stoplist
         */
        public void readStoplist(Path file) throws IOException
        {
            System.out.println("readStoplist: " + file.toString());
            BufferedReader br = new BufferedReader(new FileReader(file.toString()));
            String line = null;
            while ((line = br.readLine()) != null) 
            {
              line = line.toLowerCase();            
              stoplist.add(line);
            }
            br.close();
        }        
 

        /**
         * Map task. Read each TREC document, emit term-pair and minimum frequency     
         * @param key   Document identifier
         * @param value WARC record
         */
        public void map(LongWritable key, TrecDocument doc, Context context) throws IOException, InterruptedException 
        {            
            java.util.Map<String, Integer> docTF = new TreeMap<String, Integer>();

            // Get text from HTML 
            // TODO: This is GOV2 specific at the moment
            String text = TrecUtils.getTextFromHtml(doc.getContent());
            
            // Tokenize and keep track of term frequencies
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

            if (docLen > 0) 
            {
               
                // For each query term, for each document term, 
                // emit the term pair and min(freq(qw), freq(dw))
                for (String qterm: queryTerms)
                {
                    if (stoplist.contains(qterm))
                        continue;
                    
                    for (String docterm: docTF.keySet()) {
                        if (qterm.equals(docterm))
                            continue;
                        
                        // Map key
                        termPair.set(qterm + "\t" + docterm);
                        
                        int qtermFreq = 0;
                        if (docTF.get(qterm) != null)
                            qtermFreq = docTF.get(qterm);

                        int doctermFreq = 0;
                        if (docTF.get(docterm) != null)
                            doctermFreq = docTF.get(docterm);

                        // Only emit terms that co-occur
                        if (qtermFreq > 0 && doctermFreq > 0)
                        {
    
                            int minfreq = Math.min(qtermFreq,  doctermFreq);                            
                            
                            freq.set(minfreq);
                            context.write(termPair, freq);
                        }
                    }
                }
            }
        }
    }
    


    public static class TrecQueryStatsReducer extends Reducer<Text, IntWritable, Text, Text> 
    {
        Text output = new Text();
        
        /**
         * Reduce: Output term pair, document co-occurrences, and frequencies
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException 
            long sum = 0;
            long count = 0;
            for (IntWritable value: values) {
                sum += value.get();
                count++;
            }
            
            output.set(count + "\t" + sum);
            context.write(key, output);
        }
    }

    public int run(String[] args) throws Exception 
    {
        
        String collectionPath = args[0];
        String topicFile = args[1];
        String stoplistFile = args[2];
        String outputPath = args[3];

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "trec-query-stats");
        job.setJarByClass(TrecQueryStats.class);
        job.setMapperClass(TrecQueryStatsMapper.class);
        job.setReducerClass(TrecQueryStatsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TrecDocumentInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        DistributedCache.addCacheFile(new Path(topicFile).toUri(), job.getConfiguration());
        DistributedCache.addCacheFile(new Path(stoplistFile).toUri(), job.getConfiguration());
        
        // Input path is path to filtered ClueWeb data
        FileInputFormat.addInputPath(job, new Path(collectionPath));  
        // Path to write results
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
                       
        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new TrecQueryStats(), args);
       
       System.exit(res);
    }
}