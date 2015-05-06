package edu.gslis.hbase.trec;


import java.io.Serializable;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;





/**
 * Simplified serializable FeatureVector
 * 
 * @author Miles Efron
 */
public class FeatureVector  implements Serializable {
    
    private static final long serialVersionUID = -5444085221137694008L;
	private Map<String, Double> features;
	private double length = 0.0;

	public FeatureVector() {
		features = new HashMap<String,Double>();
	}


	public void addTerm(String term) {

		Double freq = ((Double)features.get(term));
		if(freq == null) {
			features.put(term, new Double(1.0));
		} else {
			double f = freq.doubleValue();
			features.put(term, new Double(f+1.0));
		}
		length += 1.0;
	}

	public void setTerm(String term, double weight) {
		length += weight;
		features.put(term, new Double(weight));
		
	}


	public void addTerm(String term, double weight) {
		Double w = ((Double)features.get(term));
		if(w == null) {
			features.put(term, new Double(weight));
		} else {
			double f = w.doubleValue();
			features.put(term, new Double(f+weight));
		}
		length += weight;
	}
	
	public void clip(int k) {
		List<KeyValuePair> kvpList = getOrderedFeatures();

		Iterator<KeyValuePair> it = kvpList.iterator();
		
		Map<String,Double> newMap = new HashMap<String,Double>(k);
		int i=0;
		length = 0;
		while(it.hasNext()) {
			if(i++ >= k)
				break;
			KeyValuePair kvp = it.next();
			length += kvp.getScore();
			newMap.put((String)kvp.getKey(), kvp.getScore());
		}

		features = (HashMap<String, Double>) newMap;

	}

	public void normalize() {
		Map<String,Double> f = new HashMap<String,Double>(features.size());

		double sum = 0.0;
		
		Iterator<String> it = features.keySet().iterator();
		while(it.hasNext()) {
			String feature = it.next();
			double obs = features.get(feature);
			sum += obs;
		}
		
		it = features.keySet().iterator();
		while(it.hasNext()) {
			String feature = it.next();
			double obs = features.get(feature);
			f.put(feature, obs/sum);
		}
		
		features = f;
		length = 1.0;
	}
	

	

	// ACCESSORS

	public Set<String> getFeatures() {
		return features.keySet();
	}

	public double getLength() {
		return length;
	}

	public int getFeatureCount() {
		return features.size();
	}

	public double getFeatureWeight(String feature) {
		Double w = (Double)features.get(feature);
		return (w==null) ? 0.0 : w.doubleValue();
	}

	public Iterator<String> iterator() {
		return features.keySet().iterator();
	}

	public boolean contains(Object key) {
		return features.containsKey(key);
	}



	private List<KeyValuePair> getOrderedFeatures() {
		List<KeyValuePair> kvpList = new ArrayList<KeyValuePair>(features.size());
		Iterator<String> featureIterator = features.keySet().iterator();
		while(featureIterator.hasNext()) {
			String feature = featureIterator.next();
			double value   = features.get(feature);
			KeyValuePair keyValuePair = new KeyValuePair(feature, value);
			kvpList.add(keyValuePair);
		}
		ScorableComparator comparator = new ScorableComparator(true);
		Collections.sort(kvpList, comparator);

		return kvpList;
	}

	public String toString(int k) {
		DecimalFormat format = new DecimalFormat("#.#########");
		StringBuilder b = new StringBuilder();
		List<KeyValuePair> kvpList = getOrderedFeatures();
		Iterator<KeyValuePair> it = kvpList.iterator();
		int i=0;
		while(it.hasNext() && i++ < k) {
			KeyValuePair pair = it.next();
			b.append(format.format(pair.getScore()) + " " + pair.getKey() + "\n");
		}
		return b.toString();

	}




	public static FeatureVector interpolate(FeatureVector x, FeatureVector y, double xWeight) {
		FeatureVector z = new FeatureVector();
		Set<String> vocab = new HashSet<String>();
		vocab.addAll(x.getFeatures());
		vocab.addAll(y.getFeatures());
		Iterator<String> features = vocab.iterator();
		while(features.hasNext()) {
			String feature = features.next();
			double weight  = 0.0;
			if(xWeight > 0) {
				weight = xWeight*x.getFeatureWeight(feature) + (1.0-xWeight)*y.getFeatureWeight(feature);
			} else {
				weight = x.getFeatureWeight(feature) + y.getFeatureWeight(feature);
			}
			z.addTerm(feature, weight);
		}
		return z;
	}

	public FeatureVector deepCopy() {
		FeatureVector copy = new FeatureVector();
		Iterator<String> terms = features.keySet().iterator();
		while(terms.hasNext()) {
			String term = terms.next();
			double weight = features.get(term);
			copy.addTerm(term, weight);
		}
		return copy;
	}



}
