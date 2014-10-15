package edu.gslis.hadoop;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.Scanner;

import nl.utwente.mirex.util.WarcTextConverterInputFormat;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Calculate collections statistics for complete ClueWeb09 corpus.
 * 
 * Input: 
 *  Path to unfiltered ClueWeb09 corpus in warc.gz foramt
 *  
 * Map: 
 *  Reads each document using Twente WarcTextConverterInputFormat
 *  Calculates term frequency
 *  emit(term, tf)
 *      
 * Reduce: Sums total term frequencies for each term/document pair
 *  For space efficiency, only emit if sum > 1
 *  
 * Output: 
 *      total number of documents
 *      total unique terms
 *      total terms
 *      
 */
// hadoop fs -rm -r /home/willis8/trec/clueweb09/stats
// hadoop jar hadoop-ir-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
//   edu.gslis.hadoop.ClueWebCollectionStats \
//       -D mapreduce.job.reduces=10 -D mapred.child.java.opts=-Xmx2g \
//       /trec/clueweb09/ClueWeb09_English_1/*/*
//       /home/willis8/trec/clueweb09/stats 

public class ClueWebCollectionStats extends Configured implements Tool 
{

    static enum CollectionStats {
        numberOfDocuments,
        numberOfTerms,
        numberOfTokens
    }

    public static class CWCSMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private static final String TOKENIZER = "[^0-9A-Za-z]+";

        Text termKey = new Text();
        IntWritable freq = new IntWritable();
        
        /**
         * @param key   Document identifier
         * @param key   Text extracted from WARC
         */
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException 
        {            
            // Global counter for the number of documents processed
            context.getCounter(CollectionStats.numberOfDocuments).increment(1L);
            
            Scanner scan = new Scanner(value.toString().toLowerCase()).useDelimiter(TOKENIZER);
            Bag bag = new HashBag();
            while (scan.hasNext()) 
            {
                String term = scan.next();
                bag.add(term);
                
                // Global counter for the number of tokens processed
                context.getCounter(CollectionStats.numberOfTokens).increment(1L);
            }

            // For each term in this document, collect the term and frequency
            for (Object term: bag.uniqueSet()) {
                termKey.set((String)term);
                freq.set(bag.getCount(term));
                context.write(termKey, freq);
            }
        }
    }

    public static class CWCSReducer extends Reducer<Text, IntWritable, Text, Text> 
    {
        private Text result = new Text();
        
        /**
         * @param key       Term
         * @param values    Iterable containing one int representing frequency for each document
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException 
        {
            // Global counter for the number of terms processed
            context.getCounter(CollectionStats.numberOfTerms).increment(1L);

            int sum = 0;
            int docs = 0;
            for (IntWritable val : values) {
                sum += val.get();
                docs++;
            }
            
            // Only collect terms with total frequency > 1.
            if (sum > 1) {
                result.set(docs + "," + sum);
                context.write(key, result);
            }
        }
    }

    public int run(String[] args) throws Exception 
    {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "clueweb-collection-stats");
        job.setJarByClass(ClueWebCollectionStats.class);
        job.setMapperClass(CWCSMapper.class);
        job.setReducerClass(CWCSReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WarcTextConverterInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // Temporarary directory will contain output of reduce task
        Random random = new Random();
        Path tmpOut = new Path(args[1] +  "." + Math.abs(random.nextInt()) + ".tmp");   
        FileOutputFormat.setOutputPath(job, tmpOut);
        job.waitForCompletion(true);
        
        // Read all files in the temporary directory and convert to final stats format
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path output = new Path(args[1]);
        hdfs.mkdirs(output);
        FSDataOutputStream dos = hdfs.create(new Path(args[1] + "/stats.out"));
        dos.writeBytes("#INPUT=" + args[0] + "\n");
        dos.writeBytes("#DOCS=" + job.getCounters().findCounter(CollectionStats.numberOfDocuments).getValue() + "\n");       
        dos.writeBytes("#TERMS=" + job.getCounters().findCounter(CollectionStats.numberOfTerms).getValue() + "\n");
        dos.writeBytes("#TOKENS=" + job.getCounters().findCounter(CollectionStats.numberOfTokens).getValue() + "\n");
        
        int c;
        FileStatus[] status = hdfs.listStatus(tmpOut);       
        for (int i=0;i<status.length;i++)
        {
            String fileName = status[i].getPath().getName();
            if (!fileName.startsWith("part")) continue;             
            FSDataInputStream dis = hdfs.open(status[i].getPath());
            InputStreamReader isr = new InputStreamReader(dis);
            while ((c = isr.read()) != -1) 
                dos.write(c);
            dis.close();
        }
        dos.close();
        // Remove the temporary directory
        hdfs.delete(tmpOut);
               
        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new ClueWebCollectionStats(), args);
       
       System.exit(res);
    }
}
