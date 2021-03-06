	package edu.gslis.hadoop.trec;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.lemurproject.kstem.KrovetzStemmer;
import org.lemurproject.kstem.Stemmer;

import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;

/**
 * Count total word frequencies in a TREC document collection.
 * Assumes a single input file of TREC-text formatted XML.  
 */
public class TrecWordCount2 extends Configured implements Tool 
{
	// Counter to track the number of documents and terms in the input file
	public static enum Count { DOCS  };

	private static IntWritable one = new IntWritable(1);


	/**
	 * Mapper implementation: given an input TrecDocument (Cloud9), 
	 * tokenize using the Lucene StandardAnalyzer, tally individual
	 * term frequencies.
	 */
	public static class TrecWordCountMapper extends Mapper <LongWritable, TrecDocument, Text, IntWritable> 
	{
		Text term = new Text();

        Stemmer stemmer = new KrovetzStemmer();
        Pattern numberMatcher = Pattern.compile("\\d+(\\.\\d+)?");


		public void map(LongWritable key, TrecDocument doc, Context context) 
						throws IOException, InterruptedException
		{			
			context.getCounter(Count.DOCS).increment(1);

			
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

	    	
	    	for (String word: stemmed) {
		        term.set(word);
		        context.write(term, one);
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
	 * Reducer implementation: The key is a single term, the value is a set of 
	 * frequencies for the term from all of the input documents.
	 */
	public static class TrecWordCountReducer extends Reducer <Text, IntWritable, Text, LongWritable> 
	{		
		private LongWritable sum = new LongWritable();
		private int MIN_OCCUR = 2;

	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	            throws IOException, InterruptedException 
	    {
			int cnt = 0;
			for (IntWritable val : values) {
				cnt += val.get();
			} 
			sum.set(cnt);
			
			if (cnt > MIN_OCCUR)
				context.write(key, sum);
	    }
	}
	
	public int run(String[] args) throws Exception 
	{
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TrecWordCount2.class);
		job.setInputFormatClass(TrecDocumentInputFormat.class);
		job.setMapperClass(TrecWordCountMapper.class);
		job.setReducerClass(TrecWordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		return 0;	
	}
	
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(), new TrecWordCount2(), args);
	}
}

