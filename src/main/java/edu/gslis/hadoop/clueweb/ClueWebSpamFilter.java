package edu.gslis.hadoop.clueweb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cmu.lemurproject.WarcFileInputFormat;
import edu.cmu.lemurproject.WritableWarcRecord;
//import org.apache.hadoop.filecache.DistributedCache;

/**
 * MapReduce job to filter the ClueWeb09 collection using the
 * Waterloo Fusion spam scores.
 */
public class ClueWebSpamFilter extends Configured implements Tool 
{
    static enum FilterStats {
        numberKept,
        numberWithoutSpamScore,
        numberFiltered
    }
    static Connection conn = null;
    static PreparedStatement statement = null;
    
    public static class CWSpamFilterMapper extends Mapper<LongWritable, WritableWarcRecord, 
        LongWritable, WritableWarcRecord>
    {
        //Map<String, Integer> spamScores = new HashMap<String, Integer>();
        
        public void setup(Context context) throws IOException {
            try {
                Class.forName("org.h2.Driver");
                conn = DriverManager.getConnection("jdbc:h2:tcp://gchead/./clueweb09spam");
                statement = conn.prepareStatement("select percentile_score from waterloo_spam where clueweb_docid = ?");
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);

            }
        }
        
        public void cleanup(Context context) throws IOException {
            try {
                statement.close();
                conn.close();
            } catch (Exception e) {
                throw new IOException(e.getMessage(), e);
            }
        }

        public int getSpamScore(String docno) throws IOException {
            int score = -1;
            try {
                statement.setString(1,  docno);
                ResultSet rs = statement.executeQuery();
                if (rs.next())
                    score = rs.getInt(1);
                rs.close(); 
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException(e.getMessage(), e);
            }
            return score;
        }
        /*
        public void setup(Context context) {
            Path[] waterLooFiles;
            try {
              waterLooFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
              for (Path file: waterLooFiles) {
                  BufferedReader br = new BufferedReader(new FileReader(file.toString()));
                  String line;
                  while ((line = br.readLine()) != null) {
                      String[] fields = line.split(" ");
                      spamScores.put(fields[1], Integer.parseInt(fields[1]));
                  }
                  br.close();
              }
            } catch (IOException ioe) {
              System.err.println(StringUtils.stringifyException(ioe));
              System.exit(1);
            }
          }
          */
        public void map(LongWritable key, WritableWarcRecord value, Context context) 
                throws IOException, InterruptedException 
        {            
            // Lookup spam score.  Only pass to reducer if > threshold
            String recordId = value.getRecord().getHeaderMetadataItem("WARC-TREC-ID");
 //           System.out.println(recordId);
            int score = getSpamScore(recordId);
            //if (spamScores.get(recordId) != null) {
            //    if (spamScores.get(recordId) >= 70) {
            if (score != -1) {
                if (score >= 70) {
                    context.write(key, value);
                    context.getCounter(FilterStats.numberKept).increment(1L);
                }
                else 
                    context.getCounter(FilterStats.numberFiltered).increment(1L);
            }
            else
                context.getCounter(FilterStats.numberWithoutSpamScore).increment(1L);

        }
    }

    public static class CWSpamFilterReducer extends Reducer<LongWritable, WritableWarcRecord, LongWritable, WritableWarcRecord> 
    {
        public void reduce(LongWritable key, Iterable<WritableWarcRecord> values, Context context) 
                throws IOException, InterruptedException 
        {
            Iterator<WritableWarcRecord> it = values.iterator();
            while (it.hasNext())
                context.write(key,  it.next());
        }
    }

    public int run(String[] args) throws Exception 
    {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "clueweb09-spam-filter");
        job.setJarByClass(ClueWebSpamFilter.class);
        job.setMapperClass(CWSpamFilterMapper.class);
        job.setReducerClass(CWSpamFilterReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(WritableWarcRecord.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(WritableWarcRecord.class);
        job.setInputFormatClass(WarcFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);        
        
        // Add path to Waterloo spam scores for side-loading
        //DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());

        // Input path is path to ClueWeb data
        FileInputFormat.addInputPath(job, new Path(args[0]));  
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
                       
        System.out.println("Kept " + job.getCounters().findCounter(FilterStats.numberKept).getValue());
        System.out.println("Filtered " + job.getCounters().findCounter(FilterStats.numberFiltered).getValue());
        System.out.println("No score " + job.getCounters().findCounter(FilterStats.numberWithoutSpamScore).getValue());
        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new ClueWebSpamFilter(), args);
       
       System.exit(res);
    }
}