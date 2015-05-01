package edu.gslis.hbase.trec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.collections.Bag;
import org.apache.commons.collections.bag.HashBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;


/**
 * MapReduce job to convert documents in Trec-text format to Hbase document vectors
 *      docno timestamp vector (bag)
 */
public class TrecToHBase extends Configured implements Tool 
{

    static Pattern  tagsPat 
        = Pattern.compile("<[^>]+>", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);


    public static class TrecTextMapper extends Mapper <LongWritable, TrecDocument, Text, Text> 
    {
        Text docId = new Text();
        Text text = new Text();

        public void map(LongWritable key, TrecDocument doc, Context context) 
                        throws IOException, InterruptedException
        {           

            String id = doc.getDocid();
            String content = getText(doc);
            
            docId.set(id);
            text.set(content);
            
            context.write(docId, text);             
        }
        
        /**
         * Get the text element
         */
        private static String getText(TrecDocument doc) {

            String content = doc.getContent();
            
            content = tagsPat.matcher(content).replaceAll(" ");
            
            return content;
        }
    }
    
    public static class TrecTextReducer extends TableReducer <Text, Text, ImmutableBytesWritable> 
    {       

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException 
        {
            for (Text value : values) {
                
                String docId = key.toString();
                byte[] docVector = getDocVector(value);
                Put put = new Put(Bytes.toBytes(docId));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("dv"), docVector);
                context.write(null, put);
            }
        }
        
        public byte[] getDocVector(Text text) {
            Bag bag = new HashBag();
            
            StringTokenizer tok = new StringTokenizer(text.toString());
            while (tok.hasMoreTokens()) {
                bag.add(tok.nextToken());
            }
            
            byte[] bytes = null;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = null;
            try {
              out = new ObjectOutputStream(bos);   
              out.writeObject(bag);
              bytes = bos.toByteArray();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
              try {
                if (out != null) {
                  out.close();
                }
              } catch (IOException ex) {
                // ignore close exception
              }
              try {
                bos.close();
              } catch (IOException ex) {
                // ignore close exception
              }
        }
            return bytes;

        }
    }
      
  public int run(String[] args) throws Exception 
  {
      Configuration config = HBaseConfiguration.create();
      Job job = Job.getInstance(config);
      job.setJarByClass(TrecToHBase.class); 

      job.setInputFormatClass(TrecDocumentInputFormat.class);

      Scan scan = new Scan();
      scan.setCaching(500);     
      scan.setCacheBlocks(false);

      TableMapReduceUtil.initTableReducerJob(
          "test",        // output table
          TrecTextReducer.class,    // reducer class
          job);
      job.setNumReduceTasks(1);   // at least one, adjust as required

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(TrecTextMapper.class);
//      job.setCombinerClass(MyTableReducer.class);
//      job.setReducerClass(MyTableReducer.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      
      boolean b = job.waitForCompletion(true);
      if (!b) {
          throw new IOException("error with job!");
      }
      return 0;
  }

  public static void main(String[] args) throws Exception {
      
    int res = ToolRunner.run(new Configuration(), new TrecToHBase(), args);
    System.exit(res);
  }
}