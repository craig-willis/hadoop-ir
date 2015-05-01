package edu.gslis.hadoop.trec;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.gslis.hadoop.trec.XmlInputFormat.XmlRecordReader;
import edu.umd.cloud9.collection.trec.TrecDocument;

/**
 * Copied from cloud9
 */
public class TrecDocumentInputFormat extends
    IndexableFileInputFormat<LongWritable, TrecDocument> {

	@Override
	public RecordReader<LongWritable, TrecDocument> getRecordReader(
			InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		return new TrecDocumentRecordReader((FileSplit)split, job);
	}	
	

  public static class TrecDocumentRecordReader implements
      RecordReader<LongWritable, TrecDocument> 
  {
	  private XmlRecordReader reader = null;
	  
	  public TrecDocumentRecordReader(FileSplit split, JobConf conf) throws IOException
	  {
	      conf.set(XmlInputFormat.START_TAG_KEY, TrecDocument.XML_START_TAG);
	      conf.set(XmlInputFormat.END_TAG_KEY, TrecDocument.XML_END_TAG);
		  reader = new XmlRecordReader(split, conf);
	  }
    
	  public boolean next(LongWritable key, TrecDocument doc) throws IOException {
		  Text text = new Text();
		  boolean ret = reader.next(key, text);
		  TrecDocument.readDocument(doc, text.toString());
		  return ret;
	  }
	  public LongWritable createKey() {
		return reader.createKey();
	  }
	  
	  public TrecDocument createValue() {
		  return new TrecDocument();
	  }
	  public long getPos() throws IOException {
		return reader.getPos();
	  }
	  public void close() throws IOException {
		reader.close();
	  }
	
	  public float getProgress() throws IOException {
		return reader.getProgress();
	  }
   }
}
