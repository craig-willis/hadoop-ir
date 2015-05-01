package edu.gslis.hadoop.trec;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;


/**
 * Convert a set of files from TREC-text format to Mallet import format
 */
public class TrecToMallet extends Configured implements Tool 
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
	

	public static class TrecTextReducer extends Reducer <Text, Text, Text, Text> 
	{		

	    public void reduce(Text key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException 
	    {
			for (Text value : values)
                context.write(key, value);
	    }
	}
	
	public int run(String[] args) throws Exception 
	{
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TrecToMallet.class);
		
		job.setInputFormatClass(TrecDocumentInputFormat.class);
		job.setMapperClass(TrecTextMapper.class);
		job.setReducerClass(TrecTextReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
	    job.setOutputFormatClass(TextOutputFormat.class);

	    TrecDocumentInputFormat.addInputPath(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.waitForCompletion(true);
	    
		return 0;	
	}
	
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(), new TrecToMallet(), args);
	}
}

