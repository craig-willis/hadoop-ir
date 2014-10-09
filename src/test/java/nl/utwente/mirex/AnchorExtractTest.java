package nl.utwente.mirex;

import java.util.ArrayList;

import java.util.List;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

import edu.cmu.lemurproject.WritableWarcRecord;
import edu.cmu.lemurproject.WarcRecord;

import nl.utwente.mirex.AnchorExtract;
 
public class AnchorExtractTest {
 
  private MapDriver<LongWritable, WritableWarcRecord, Text, Text> mapDriver;
  private ReduceDriver<Text, Text, Text, Text> reduceDriver;
  private ReduceDriver<Text, Text, Text, Text> combineDriver;
  private MapReduceDriver<LongWritable, WritableWarcRecord, Text, Text, Text, Text> mapReduceDriver; 
 
  @Before
  public void setUp() {
    AnchorExtract.Map mapper      = new AnchorExtract.Map();
    AnchorExtract.Reduce reducer  = new AnchorExtract.Reduce();
    AnchorExtract.Combine combiner  = new AnchorExtract.Combine();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    combineDriver = ReduceDriver.newReduceDriver(combiner);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }
 

  @Test
  public void testMapper() {
    LongWritable key = new LongWritable(1234);
    WarcRecord record = new WarcRecord();
    record.setWarcRecordType("response");
    record.addHeaderMetadata("WARC-Target-URI", "http://utwente.nl");
    record.addHeaderMetadata("WARC-TREC-ID", "TREC0001");
    record.setContent("<html><a href='http://mirex.sf.net'>MIREX <!-- test -->rocks</a>!</html>");
    WritableWarcRecord value = new  WritableWarcRecord(record);
    mapDriver.withInput(key, value);
    mapDriver.withOutput(new Text("http://utwente.nl"), new Text("MIREX-TREC-ID: TREC0001"));
    mapDriver.withOutput(new Text("http://mirex.sf.net"), new Text("MIREX  rocks"));
    mapDriver.runTest();
  }
 

  @Test
  public void testReducer() {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("University of Twente"));
    values.add(new Text(AnchorExtract.MirexId + "TREC0001"));
    values.add(new Text("UT"));
    reduceDriver.withInput(new Text("http://utwente.nl"), values);
    reduceDriver.withOutput(new Text("TREC0001"), new Text("http://utwente.nl\tUniversity of Twente\tUT"));
    reduceDriver.runTest();
  }


    @Test
    public void testCombiner() {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("University of Twente"));
    values.add(new Text(AnchorExtract.MirexId + "TREC0001"));
    values.add(new Text("UT"));
    combineDriver.withInput(new Text("http://utwente.nl"), values);
    combineDriver.withOutput(new Text("http://utwente.nl"), new Text(AnchorExtract.MirexId + "TREC0001"));
    combineDriver.withOutput(new Text("http://utwente.nl"), new Text("University of Twente\tUT"));
    combineDriver.runTest();
  }


}
