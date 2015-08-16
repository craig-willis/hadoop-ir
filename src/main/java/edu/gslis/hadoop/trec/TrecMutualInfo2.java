package edu.gslis.hadoop.trec;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.lemurproject.kstem.KrovetzStemmer;
import org.lemurproject.kstem.Stemmer;

import edu.gslis.hadoop.trec.TrecWordCount2.TrecWordCountMapper;
import edu.gslis.hadoop.trec.TrecWordCount2.TrecWordCountReducer;
import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;



public class TrecMutualInfo2 extends Configured implements Tool 
{
    private static final Logger logger = Logger.getLogger(TrecMutualInfo2.class);

    public static class TrecMutualInfoMapper extends Mapper <LongWritable, TrecDocument, Text, MapWritable> 
    {
      private final static IntWritable one = new IntWritable(1);
      Text term1 = new Text();
      
      Stemmer stemmer = new KrovetzStemmer();
      Pattern numberMatcher = Pattern.compile("\\d+(\\.\\d+)?");
      Set<String> wordList = new HashSet<String>();


      public void readWordList(String path, FileSystem fs) 
      {
          logger.info("Reading word list from: " + path);

          try
          {
              BufferedReader br = null;
              if (fs == null)  {
                  br = new BufferedReader(new FileReader(path));
              } else {
                  DataInputStream dis = fs.open(new Path(path));
                  br = new BufferedReader(new InputStreamReader(dis));
              }
              String line;
              while ((line = br.readLine()) != null) {
                  wordList.add(line);
              }
              br.close();
          } catch (Exception e) {
              e.printStackTrace();
          }
          logger.info("Reading " + wordList.size() + " words from word list");

      }
      
      protected void setup(Context context) 
      {
          try {
              FileSystem fs = FileSystem.get(context.getConfiguration());

              // Read the DocumentWordCount output file
              URI[] files = context.getCacheFiles();
              if (files != null) {
                  for (URI file: files) {
                      if (file.toString().contains("mutual-info-words"))
                          readWordList(file.getPath(), fs);
                  }
              }
              else {
                  logger.error("Can't load cache files. Trying local cache");
                  Path[] paths = context.getLocalCacheFiles();
                  for (Path path: paths) {
                      if (path.toString().contains("mutual-info-words"))
                          readWordList(path.toString(), null);
                  }
              }
          } catch (Exception ioe) {
              ioe.printStackTrace();
              logger.error(ioe);
          }            

      } 
      /**
       * Get the text element
       */
      private static String getText(TrecDocument doc) {

          String text = "";
          String content = doc.getContent();
          int start = content.indexOf("<TEXT>");
          if (start == -1) {
              text = "";
          } else {
              int end = content.indexOf("</TEXT>", start);
              text= content.substring(start + 6, end).trim();
          }
          return text;
      }
      public void map(LongWritable key, TrecDocument doc, Context context) 
              throws IOException, InterruptedException
      {
          String line = getText(doc);
          
          line = line.replaceAll("[^A-Za-z0-9]", " ");
          line = line.toLowerCase();
        
          StringTokenizer tokenizer = new StringTokenizer(line);
          List<String> stemmed = new ArrayList<String>();
          while (tokenizer.hasMoreTokens()) {
              String w = tokenizer.nextToken();
              if (! w.matches("[0-9]+")) {
                  stemmed.add(stemmer.stem(w));
              }
          }
          
          for (String word1: stemmed) {
              // Create an associative array containing all 
              // co-occurring words.  Note: this is symmetric, 
              // but shouldn't effect MI values.
              
              // If wordList provided, only collect terms for those words in the list
              if ((wordList.size() > 0) && (!wordList.contains(word1)))
                  continue;
              
              MapWritable map = new MapWritable();
              term1.set(word1);
              
              for (String word2: stemmed) {
                  
                  if (word1.equals(word2))
                      continue;
                  
                  Text term2 = new Text();

                  term2.set(word2);
                  map.put(term2, one);
              }
              context.write(term1, map);
          }          
      
       }
    }

    public static class TrecMutualInfoReducer extends Reducer <Text, MapWritable, Text, DoubleWritable> 
    {
    
        Map<String, Integer> documentFreq = new HashMap<String, Integer>();
        Text wordPair = new Text();
        DoubleWritable mutualInfo = new DoubleWritable();
        int totalNumDocs = 0;


        public void readDocFreq(String path, FileSystem fs) 
        {
            logger.info("Reading total word counts from: " + path);

            try
            {
                BufferedReader br = null;
                if (fs == null)  {
                    br = new BufferedReader(new FileReader(path));
                } else {
                    DataInputStream dis = fs.open(new Path(path));
                    br = new BufferedReader(new InputStreamReader(dis));
                }
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split("\t");
                    documentFreq.put(fields[0], Integer.parseInt(fields[1]));
                }
                br.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            logger.info("Read " + documentFreq.size() + " words");
        }


        /**
         * Side-load the output from TrecWordCount job
         */
        protected void setup(Context context) 
        {   
            logger.info("Setup");

            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());

                // Read the DocumentWordCount output file
                URI[] files = context.getCacheFiles();
                if (files != null) {
                    for (URI file: files) {
                        if (file.toString().contains("mutual-info-words"))
                            continue;
                        readDocFreq(file.getPath(), fs);
                    }
                }
                else {
                    logger.error("Can't load cache files. Trying local cache");
                    Path[] paths = context.getLocalCacheFiles();
                    for (Path path: paths) {
                        if (path.toString().contains("mutual-info-words"))
                            continue;
                        
                        readDocFreq(path.toString(), null);
                    }
                }
            } catch (Exception ioe) {
                ioe.printStackTrace();
                logger.error(ioe);
            }
        }

        public void reduce(Text term, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException 
        {

            // key contains a given word and values contains a set of
            // associative arrays containing all co-occurring words.  Each
            // value represents all co-occurring words in a single document.
            // Collect all of the co-occurrences into a single map.
            Map<String, Integer> jointOccurrences = new HashMap<String, Integer>();
            for (MapWritable map: values) 
            {
                Set<Writable> keys = map.keySet();
                for (Writable key: keys)
                {
                    IntWritable count = (IntWritable)map.get(key);
                    String word2 = key.toString();
                    
                    if (jointOccurrences.containsKey(word2)) {
                        int sum = jointOccurrences.get(word2);
                        sum += count.get();
                        jointOccurrences.put(word2, sum);
                    } else {
                        jointOccurrences.put(word2, count.get());
                    }
                }
            }
    
            // For each word pair, calculate EMIM.
            String word1 = term.toString();
            for (String word2: jointOccurrences.keySet()) 
            {
                if (documentFreq.containsKey(word1) && 
                        documentFreq.containsKey(word2))
                {
                    //        | wordY | ~wordY |
                    // -------|-------|--------|------
                    //  wordX | nX1Y1 | nX1Y0  | nX1
                    // ~wordX | nX0Y1 | nX0Y0  | nX0
                    // -------|-------|--------|------
                    //        |  nY1  |  nY0   | total
    
                    double nX1Y1 = jointOccurrences.get(word2);
                    double nX1 = documentFreq.get(word1);
                    double nY1 = documentFreq.get(word2);
    
                    //logger.info(word1 + "," + word2 + "," + totalNumDocs + "," + nX1Y1 + "," + nX1 + "," + nY1);
                    //double emim = calculateEmim(totalNumDocs, nX1Y1, nX1, nY1);
                    double npmi = calculateNPMI(totalNumDocs, nX1Y1, nX1, nY1);
                    
                    
                    wordPair.set(word1 + "\t" + word2);
                    mutualInfo.set(npmi);
                    context.write(wordPair, mutualInfo);
                }
            }
        }
        
        private double calculateNPMI(double N, double nX1Y1, double nX1, double nY1)
        {
            
            //        | wordY | ~wordY |
            // -------|-------|--------|------
            //  wordX | nX1Y1 | nX1Y0  | nX1
            // ~wordX | nX0Y1 | nX0Y0  | nX0
            // -------|-------|--------|------
            //        |  nY1  |  nY0   | gt
    
    
            // Marginal probabilities (smoothed)
            double pX1 = (nX1 + 0.5)/(1+N);
            double pY1 = (nY1 + 0.5)/(1+N);
            
            // Joint probabilities (smoothed)
            double pX1Y1 = (nX1Y1 + 0.25) / (1+N);
            
            // Ala http://www.aclweb.org/anthology/W13-0102
            double pmi = log2(pX1Y1, pX1*pY1);
            double npmi = pmi / Math.log(pX1Y1)/Math.log(2);
            
            return npmi;
        }

    }

    private static double log2(double num, double denom) {
        if (num == 0 || denom == 0)
            return 0;
        else
            return Math.log(num/denom)/Math.log(2);
    }
    

    public int run(String[] args) throws Exception 
    {
        Path inputPath = new Path(args[0]);
        Path wcOutputPath = new Path(args[1]);
        Path miOutputPath = new Path(args[2]);

        Job wc =  Job.getInstance(getConf(), "trec-word-count-2");
          
        wc.setJarByClass(TrecWordCount2.class);
        wc.setMapperClass(TrecWordCountMapper.class);
        wc.setReducerClass(TrecWordCountReducer.class);
          
        wc.setInputFormatClass(TrecDocumentInputFormat.class);
        wc.setOutputFormatClass(TextOutputFormat.class);
          
        wc.setMapOutputKeyClass(Text.class);
        wc.setMapOutputValueClass(IntWritable.class);
            
        wc.setOutputKeyClass(Text.class);
        wc.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.setInputPaths(wc, inputPath);
        FileOutputFormat.setOutputPath(wc, wcOutputPath);
          
        wc.waitForCompletion(true);
        Counters counters = wc.getCounters();
        int numDocs = (int) counters.findCounter(TrecWordCount2.Count.DOCS).getValue();
        
        Configuration conf = getConf();
        conf.set("numDocs", String.valueOf(numDocs));
        

        Job mi = Job.getInstance(conf, "trec-mutual-info");
        
        if (args.length == 4) {
            Path wordListPath = new Path(args[3]);
            conf.set("wordListPath", wordListPath.toUri().toString());
            mi.addCacheFile(wordListPath.toUri());
        }      
        mi.setJarByClass(TrecMutualInfo2.class);

        mi.setMapperClass(TrecMutualInfoMapper.class);
        mi.setReducerClass(TrecMutualInfoReducer.class);
      
        mi.setInputFormatClass(TrecDocumentInputFormat.class);
        mi.setOutputFormatClass(TextOutputFormat.class);
      
        mi.setMapOutputKeyClass(Text.class);
        mi.setMapOutputValueClass(MapWritable.class);
        
        mi.setOutputKeyClass(Text.class);
        mi.setOutputValueClass(DoubleWritable.class);
  
        FileInputFormat.setInputPaths(mi, inputPath);
        FileOutputFormat.setOutputPath(mi, miOutputPath);
        
        FileSystem fs = FileSystem.get(conf);
        Path pathPattern = new Path(wcOutputPath, "part-r-[0-9]*");
        FileStatus [] list = fs.globStatus(pathPattern);
        for (FileStatus status: list) {
            String name = status.getPath().toString();
            logger.info("Adding cache file " + name);
            mi.addCacheFile(new Path(wcOutputPath, name).toUri()); 
        }
        mi.waitForCompletion(true);
        
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TrecMutualInfo2(), args);
        System.exit(res);
    }
}

