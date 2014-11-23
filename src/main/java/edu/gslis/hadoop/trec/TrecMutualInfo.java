package edu.gslis.hadoop.trec;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import edu.gslis.hadoop.trec.TrecWordCount.TrecWordCountMapper;
import edu.gslis.hadoop.trec.TrecWordCount.TrecWordCountReducer;
import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;

/**
 * Given an input file of TREC-text formatted XML documents, calculate
 * pairwise mutual information for all words.
 * 
 * The run() method executes two chained jobs:
 * 	1) TrecWordCount: calculates total word frequencies for all words
 *  2) TrecMutualInfo: calculates mutual information for all word pairs
 * 
 * To run:
 * 		hadoop jar hadoop-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
 * 				edu.illinois.lis.hadoop.TrecMutualInfo \
 * 				/hdfs/path/to/concatenated/trec/docs \
 * 				/hdfs/path/to/wordcount/output /hdfs/path/to/mutualinfo/output
 */
public class TrecMutualInfo extends Configured  implements Tool 
{
	private static final Logger logger = Logger.getLogger(TrecMutualInfo.class);
	

	/**
	 * Mapper implementation: given an input TrecDocument (Cloud9), 
	 * tokenize using the Lucene StandardAnalyzer. For each word in 
	 * the input TrecDocument, tally co-occurrences.  Note: this is
	 * symmetrical, but that isn't a problem for the final mutual 
	 * information calculation.
	 * 
	 * Outputs each word (key) and an associative array (map) of all
	 * co-occurring words (value).
	 */
	public static class TrecMutualInfoMapper extends Mapper <LongWritable, TrecDocument, Text, MapWritable> 
	{
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);
		Text term1 = new Text();
		IntWritable one = new IntWritable(1);
		Pattern numberMatcher = Pattern.compile("\\d+(\\.\\d+)?");
		Set<String> wordList = new HashSet<String>();

	    protected void setup(Context context) 
	    {
	        Configuration conf = context.getConfiguration();   
            try {
                // Read the DocumentWordCount output file
                URI[] files = context.getCacheFiles();
                if (files != null) {
                    for (URI file: files) {
                        if (file.toString().contains("mutual-info-words"))
                        {
                            List<String> words = FileUtils.readLines(new File(file));
                            wordList.addAll(words);
                            System.out.println("Read " + wordList.size() + " words from wordList " + file.toString());
                        }
                    }
                }
                else {
                    logger.error("Can't load cache files. Trying local cache");
                    Path[] paths = context.getLocalCacheFiles();
                    for (Path path: paths) {
                        if (path.toString().contains("mutual-info-words"))
                        {
                            List<String> words = FileUtils.readLines(new File(path.toString()));
                            wordList.addAll(words);
                            System.out.println("Read " + wordList.size() + " words from wordList " + path.toString());
                        }
                    }
                }
            } catch (Exception ioe) {
                ioe.printStackTrace();
                logger.error(ioe);
            }            
            
            
	    }
		public void map(LongWritable key, TrecDocument doc, Context context) 
						throws IOException, InterruptedException
		{	
	        TokenStream stream = analyzer.tokenStream(null,
	                new StringReader(getText(doc)));
	        stream.reset();

	        //stream = new EnglishPossessiveFilter(Version.LUCENE_43, stream);
	        CharTermAttribute cattr = stream.addAttribute(CharTermAttribute.class);

	        Set<String> words = new HashSet<String>();
			while (stream.incrementToken()) {
				Matcher m = numberMatcher.matcher(cattr.toString());
				if (!m.find()) {
					words.add(cattr.toString());
				}
			}
			
			Iterator<String> it1 = words.iterator();
			while (it1.hasNext()) {
	    		// Create an associative array containing all 
	    		// co-occurring words.  Note: this is symmetric, 
	    		// but shouldn't effect MI values.
				String word1 = (String)it1.next();
				
                // If wordList provided, only collect terms for those words in the list
                if ((wordList.size() > 0) && (!wordList.contains(word1)))
                    continue;
				
				MapWritable map = new MapWritable();
				term1.set(word1);
				
				Iterator<String> it2 = words.iterator();
				while (it2.hasNext()) {
					String word2 = (String)it2.next();
					
					if (word1.equals(word2))
						continue;
					
					Text term2 = new Text();

					term2.set(word2);
					map.put(term2, one);
				}
				context.write(term1, map);
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
	}

	
	/**
	 * Reducer implementation: The key is a word and the value is a list of 
	 * associative arrays (map) for all co-occuring terms.  The map key is the 
	 * co-occurring term, the map value is the term frequency.
	 */
	public static class TrecMutualInfoReducer extends Reducer <Text, MapWritable, Text, DoubleWritable> 
	{		
		Map<String, Integer> documentFreq = new HashMap<String, Integer>();
		Text wordPair = new Text();
		DoubleWritable mutualInfo = new DoubleWritable();

		/**
		 * Side-load the output from TrecWordCount job
		 */
		protected void setup(Context context) 
		{	
			logger.info("Setup");
			try {
				// Read the DocumentWordCount output file
				URI[] files = context.getCacheFiles();
				if (files != null) {
					for (URI file: files) {
					    if (file.toString().contains("mutual-info-words"))
					        continue;
						logger.info("Reading total word counts from: " + file.toString());
						List<String> lines = FileUtils.readLines(new File(file));
						for (String line: lines) {
							String[] fields = line.split("\t");
							documentFreq.put(fields[0], Integer.parseInt(fields[1]));
						}
						logger.info("Read " + documentFreq.size() + " words");
					}
				}
				else {
					logger.error("Can't load cache files. Trying local cache");
					Path[] paths = context.getLocalCacheFiles();
					for (Path path: paths) {
	                    if (path.toString().contains("mutual-info-words"))
	                        continue;
						logger.info("Reading total word counts from: " + path.toString());
//						List<String> lines = FileUtils.readLines(new File(path.toUri()));
						List<String> lines = FileUtils.readLines(new File(path.toUri().toString()));
						for (String line: lines) {
							String[] fields = line.split("\t");
							documentFreq.put(fields[0], Integer.parseInt(fields[1]));
						}
						logger.info("Read " + documentFreq.size() + " words");
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
	    	logger.info("Reduce");
	    	Configuration conf = context.getConfiguration();
	    	int totalNumDocs = Integer.parseInt(conf.get("numDocs"));
	    	
			// key contains a given word and values contains a set of
			// associative arrays containing all co-occurring words.  Each
			// value represents all co-occurring words in a single document.
			// Collect all of the co-occurrences into a single map.
			Map<String, Integer> jointOccurrences = new HashMap<String, Integer>();
			Iterator<MapWritable> it = values.iterator();
			while (it.hasNext())
			{
				MapWritable map = it.next();
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

					double emim = calculateEmim(totalNumDocs, nX1Y1, nX1, nY1);
					
					wordPair.set(word1 + "\t" + word2);
					mutualInfo.set(emim);
					context.write(wordPair, mutualInfo);
				}
			}
			logger.info("Reduce done");
		}
		
	    /**
	     * The actual mutual information calculation.  Given the total
	     * number of terms (N), the joint occurrences of word1 and word2,
	     * and the marginals of word1 and word2.
	     */
		private double calculateEmim(double N, double nX1Y1, double nX1, double nY1)
		{
			
			//        | wordY | ~wordY |
			// -------|-------|--------|------
			//  wordX | nX1Y1 | nX1Y0  | nX1
			// ~wordX | nX0Y1 | nX0Y0  | nX0
			// -------|-------|--------|------
			//        |  nY1  |  nY0   | gt

			// Marginal and joint frequencies
			double nX0 = N - nX1;
			double nY0 = N - nY1;		
			double nX1Y0 = nX1 - nX1Y1;
			double nX0Y1 = nY1 - nX1Y1;
			double nX0Y0 = nX0 - nX0Y1;

			// Marginal probabilities (smoothed)
			double pX1 = (nX1 + 0.5)/(1+N);
			double pX0 = (nX0 + 0.5)/(1+N);			
			double pY1 = (nY1 + 0.5)/(1+N);
			double pY0 = (nY0 + 0.5)/(1+N);
			
			// Joint probabilities (smoothed)
			double pX1Y1 = (nX1Y1 + 0.25) / (1+N);
			double pX1Y0 = (nX1Y0 + 0.25) / (1+N);
			double pX0Y1 = (nX0Y1 + 0.25) / (1+N);
			double pX0Y0 = (nX0Y0 + 0.25) / (1+N);
			
			// 
			double emim =  
					pX1Y1 * log2(pX1Y1, pX1*pY1) + 
					pX1Y0 * log2(pX1Y0, pX1*pY0) +
					pX0Y1 * log2(pX0Y1, pX0*pY1) +
					pX0Y0 * log2(pX0Y0, pX0*pY0);
			
			return emim;
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
		  Path wordListPath = new Path(args[3]);

		  Job wc =  Job.getInstance(getConf(), "trec-word-count");
			
		  wc.setJarByClass(TrecWordCount.class);
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
		  int numDocs = (int) counters.findCounter(TrecWordCount.Count.DOCS).getValue();
		  
		  Configuration conf = new Configuration();
		  conf.set("numDocs", String.valueOf(numDocs));
		  
		  Job mi = Job.getInstance(conf, "trec-mutual-info");
		  conf.set("wordListPath", wordListPath.toUri().toString());
		  mi.addCacheFile(wordListPath.toUri());
		
		  mi.setJarByClass(TrecMutualInfo.class);

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
		  int res = ToolRunner.run(new Configuration(), new TrecMutualInfo(), args);
		  System.exit(res);
	  }
}

