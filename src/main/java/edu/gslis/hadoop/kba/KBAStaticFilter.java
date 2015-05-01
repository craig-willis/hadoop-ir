package edu.gslis.hadoop.kba;

import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.gslis.eval.FilterEvaluation;
import edu.gslis.eval.Qrels;
import edu.gslis.filtering.threshold.SimpleCutoffThresholdClassifier;
import edu.gslis.filtering.threshold.ThresholdClassifier;
import edu.gslis.filtering.threshold.ThresholdFinder;
import edu.gslis.filtering.thresholds.ThresholdFinderEmpiricalThresh;
import edu.gslis.kba.adaptation.QuantileThresholding;
import edu.gslis.searchhits.SearchHit;
import edu.gslis.searchhits.SearchHits;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;

/**
 * For KBA 2014
 * Given an input of weights and results,
 * calculates the average F1 for the weight
 */
public class KBAStaticFilter extends Configured  implements Tool 
{
	private static final Logger logger = Logger.getLogger(KBAStaticFilter.class);
	static int REL_LEVEL = 2;
	

	public static class KBAStaticFilterMapper extends Mapper <LongWritable, Text, Text, MapWritable> 
	{
        DecimalFormat df = new DecimalFormat("#.##");
		Text queryText = new Text();
		Text weightText = new Text();

		public void map(LongWritable key, Text value, Context context) 
						throws IOException, InterruptedException
		{	
            String[] fields = value.toString().split("\\t");
            String weights = fields[0];           
            String query = fields[1];           
            
            //String weights = "1 1 1 1 1 1";
            weightText.set(weights);                
            queryText.set(query);
            
            MapWritable map = new MapWritable();
            Text[] textValues = new Text[fields.length - 1];
            for (int i=2; i<fields.length; i++) {
                textValues[i-2] = new Text(fields[i]);
            }
            TextArrayWritable array = new TextArrayWritable();
            array.set(textValues);
            
            map.put(queryText, array);

            context.write(weightText, map);
		}		
	}

	public static class KBAStaticFilterReducer extends Reducer <Text, MapWritable, Text, DoubleWritable> 
	{		

	    Qrels trainQrels;
	    Qrels testQrels;
	    DoubleWritable f1Writable = new DoubleWritable();

	    
		/**
		 * Side-load the qrels
		 */
		protected void setup(Context context) 
		{	
			logger.info("Setup");
			try {
				// Read the qrels output file
				URI[] files = context.getCacheFiles();
				if (files != null) {
					for (URI file: files) {
					    if (file.toString().contains("qrels-train")) {
				            trainQrels = new Qrels(file.toString(), true, 2);
					    }
                        if (file.toString().contains("qrels-test")) {
                            testQrels = new Qrels(file.toString(), true, 2);
                        }
					}
				}
				else {
					logger.error("Can't load cache files. Trying local cache");
					Path[] paths = context.getLocalCacheFiles();
					for (Path path: paths) {
                        if (path.toString().contains("qrels-train")) {
                            trainQrels = new Qrels(path.toString(), true, 2);
                        }
                        if (path.toString().contains("qrels-test")) {
                            testQrels = new Qrels(path.toString(), true, 2);
                        }
					}
				}
			} catch (Exception ioe) {
				ioe.printStackTrace();
				logger.error(ioe);
			}
		}

	    public void reduce(Text weightText, Iterable<MapWritable> values, Context context)
	            throws IOException, InterruptedException 
	    {
	    	logger.info("Reduce");
            String[] weightStrs = weightText.toString().split(" ");
            double[] weights = new double[weightStrs.length];
            for (int i=0; i<weightStrs.length; i++)
                weights[i] = Double.parseDouble(weightStrs[i]);
            
            Map<String, Map<String, List<SearchHit>>> hits = new LinkedHashMap<String, Map<String, List<SearchHit>>>();

            Iterator<MapWritable> it = values.iterator();

            while (it.hasNext()) 
            {
                MapWritable map = it.next();
                
                Set<Writable> keys = map.keySet();
                for (Writable key: keys)
                {
                    Text queryText = (Text)key;
                    TextArrayWritable array = (TextArrayWritable)map.get(key);
                    String query = queryText.toString();

                        
                    Writable[] writables = array.get();
                    Text[] fields = new Text[writables.length];
                    
                    for (int i=0; i<writables.length; i++)
                           fields[i] = (Text)writables[i];
                    
                    String docno = fields[0].toString();
                    double score = Double.parseDouble(fields[1].toString());
                    double burstScore = Double.parseDouble(fields[2].toString());
                    double nonIndScore = Double.parseDouble(fields[3].toString());
                    double prevDocScore = Double.parseDouble(fields[4].toString());
                    double lengthScore = Double.parseDouble(fields[5].toString());
                    double verbScore = Double.parseDouble(fields[6].toString());
                    double sourceScore = Double.parseDouble(fields[7].toString());
                    String type = fields[8].toString();       
                    
                    if (Double.isInfinite(sourceScore))
                        sourceScore = 0;
                    
                    Map<String, List<SearchHit>> queryHits = hits.get(query);
                    if (queryHits == null) 
                        queryHits = new HashMap<String, List<SearchHit>>();
                    
                    List<SearchHit> trainHits = queryHits.get("train");
                    if (trainHits == null)
                        trainHits = new LinkedList<SearchHit>();
                    List<SearchHit> testHits = queryHits.get("test");
                    if (testHits == null)
                        testHits = new LinkedList<SearchHit>();
                    
                    SearchHit hit = new SearchHit();
                    hit.setDocno(docno);
                    hit.setScore(score);
                    hit.setMetadataValue("delta", burstScore);            
                    hit.setMetadataValue("nonind", nonIndScore);
                    hit.setMetadataValue("prevdoc", prevDocScore);
                    hit.setMetadataValue("loglength", lengthScore);
                    hit.setMetadataValue("verb", verbScore);
                    hit.setMetadataValue("source", sourceScore);
                    
                    if (type.equals("train"))
                        trainHits.add(hit);
                    else
                        testHits.add(hit);
                    
                    queryHits.put("train", trainHits);
                    queryHits.put("test", testHits);
                    hits.put(query, queryHits);      
                }
            }
                
            ThresholdFinder optimizer = new ThresholdFinderEmpiricalThresh();
            String[] features = {"delta", "nonind", "prevdoc", "loglength", "verb", "source" };
            double avgf1 = filter(hits, trainQrels, testQrels, optimizer, features, weights);
            
            f1Writable.set(avgf1);
            context.write(weightText, f1Writable);
	    				
			logger.info("Reduce done");
		}
	    
	    public static double filter(Map<String, Map<String, List<SearchHit>>> hits, Qrels trainQrels, Qrels testQrels,
	            ThresholdFinder optimizer, String[] features, double[] weights)  
	    {        
	        QuantileThresholding scoreStats = new QuantileThresholding();
	        ThresholdClassifier thresholder;

	        double avgF1 = 0;
	        for (String query: hits.keySet()) {
	            Map<String, List<SearchHit>> queryHits = hits.get(query);            
	            List<SearchHit> trainHits = queryHits.get("train");
	            List<SearchHit> testHits = queryHits.get("test");

	            SearchHits trainingHits = new SearchHits();
	            for (SearchHit hit: trainHits) {
	                double score = hit.getScore();
	                
	                for (int i=0; i < features.length; i++) {
	                    double featureScore = (Double)hit.getMetadataValue(features[i]);
	                    score += weights[i] * featureScore;
	                }
	                
	                hit.setScore(score);
	                
	                scoreStats.addScore(score);

	                trainingHits.add(hit); 
	            }   
	                
	            optimizer.init(query, trainingHits, trainQrels);
	            thresholder = new SimpleCutoffThresholdClassifier();
	            ((SimpleCutoffThresholdClassifier)thresholder).setThreshold(optimizer.getThreshold());

	            
	            SearchHits testingEmitted = new SearchHits();
	            for (SearchHit hit: testHits) 
	            {
	                double score = hit.getScore();

	                for (int i=0; i < features.length; i++) {
	                    double featureScore = (Double)hit.getMetadataValue(features[i]);
	                    score += weights[i] * featureScore;
	                }
	        
	                int confidence = scoreStats.getKbaIntScore(score);
	                hit.setMetadataValue("confidence", String.valueOf(confidence));
	                        
	                hit.setScore(score);
	                        
	                if(Double.isInfinite(thresholder.getThreshold()) || thresholder.emit(score)) {
	                    testingEmitted.add(hit); 
	                }
	            }   
	            
	            FilterEvaluation filterEval = new FilterEvaluation(testQrels);
	            filterEval.setResults(testingEmitted);
	            double f1 = filterEval.f1Query(query);
	            avgF1 += f1;
	           
	                   
	        }        
	        return avgF1/hits.keySet().size();
	    }     
	}
	


	
	  public int run(String[] args) throws Exception 
	  {
		  Path inputPath = new Path(args[0]);
		  Path outputPath = new Path(args[1]);
		  
		  Path trainQrelsPath = new Path(args[2]);
		  Path testQrelsPath = new Path(args[3]);

          Configuration conf = new Configuration();
		  Job filter =  Job.getInstance(conf, "kba-static-filter");
	      conf.set("trainQrelsPath", trainQrelsPath.toUri().toString());
	      filter.addCacheFile(trainQrelsPath.toUri());
          conf.set("testQrelsPath", testQrelsPath.toUri().toString());
          filter.addCacheFile(testQrelsPath.toUri());
		  
		  filter.setJarByClass(KBAStaticFilter.class);
		  filter.setMapperClass(KBAStaticFilterMapper.class);
		  filter.setReducerClass(KBAStaticFilterReducer.class);
			
		  filter.setInputFormatClass(TrecDocumentInputFormat.class);
		  filter.setOutputFormatClass(TextOutputFormat.class);
			
		  filter.setMapOutputKeyClass(Text.class);
		  filter.setMapOutputValueClass(Text.class);
			  
		  filter.setOutputKeyClass(Text.class);
		  filter.setOutputValueClass(Text.class);
		  
		  FileInputFormat.setInputPaths(filter, inputPath);
		  FileOutputFormat.setOutputPath(filter, outputPath);
			
		  filter.waitForCompletion(true);

		  return 0;
	  }
	
	  public static void main(String[] args) throws Exception {
		  int res = ToolRunner.run(new Configuration(), new KBAStaticFilter(), args);
		  System.exit(res);
	  }
}

