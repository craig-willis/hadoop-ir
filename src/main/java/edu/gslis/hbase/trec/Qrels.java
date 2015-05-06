package edu.gslis.hbase.trec;

import java.io.File;

import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

/**
 * Container for <code>trec_eval</code> -type qrels.
 * 
 * @author mefron
 *
 */
public class Qrels {

	public static final Pattern SPACE_PATTERN = Pattern.compile(" ", Pattern.DOTALL);

	// remember.  this is an assumption of format based on TREC qrels.
	private static final int QUERY_COLUMN = 0;
	private static final int DOCNO_COLUMN = 2;
	private static final int REL_COLUMN   = 3;
	
	/**
	 * A map of queryName to set_of_rel_docnos
	 */
	private Map<String,Set<String>> rel;
	
	/**
	 * A map of queryName to rel_docnos->rel_level map
	 */
	private Map<String,Map<String, Integer>> relLevels;
	
	/**
	 * A map of queryName to set_of_non_rel_docnos.  Probably unused.
	 */
	private Map<String,Set<String>> nonRel;
	
	/**
	 * needed if we want to walk over queries in order.
	 */
	private List<String> orderedQueryNames;


	public Qrels() {
		orderedQueryNames = new LinkedList<String>();
		rel = new HashMap<String,Set<String>>();
		relLevels = new HashMap<String,Map<String,Integer>>();
	}
	
	/**
	 * 
	 * @param pathToQrelsFile
	 * @param storeNonRel
	 * @param minRel           what is the minimum score for "relevance"?  e.g. 1?  2?
	 */
	public Qrels(String pathToQrelsFile, boolean storeNonRel, int minRel) {
		orderedQueryNames = new LinkedList<String>();
		try {

			rel = new HashMap<String,Set<String>>();
			relLevels = new HashMap<String,Map<String,Integer>>();

			if(storeNonRel)
				nonRel = new HashMap<String,Set<String>>();

			List<String> lines = IOUtils.readLines(new FileReader(new File(pathToQrelsFile)));
			Iterator<String> linesIt = lines.iterator();
			while(linesIt.hasNext()) {
				String line = linesIt.next();
				String[] toks = SPACE_PATTERN.split(line);
				if(toks==null || toks.length != 4) {
					System.err.println("bad qrels line: " + line + ":");
					continue;
				}
				String query = toks[QUERY_COLUMN];
				String docno = toks[DOCNO_COLUMN];
				int r = Integer.parseInt(toks[REL_COLUMN]);
				
                if (contains(docno, query))
                    System.err.println("Warning: input file contains duplicate judgments for ("  +
                    		query + "," + docno + ")");

				if(r >= minRel) {
					Set<String> relDocs = null;
					Map<String, Integer> relDocLevels = null;
					if(!rel.containsKey(query)) {
						relDocs = new HashSet<String>();
						relDocLevels = new HashMap<String, Integer>();
					} else {
						relDocs = rel.get(query);
						relDocLevels = relLevels.get(query);
					}
					
					relDocs.add(docno);
					relDocLevels.put(docno, r);
					rel.put(query, relDocs);
					relLevels.put(query, relDocLevels);
					
				} else {
					
					if(storeNonRel) 
					{
						Set<String> nonRelDocs = null;
						if(!nonRel.containsKey(query)) {
							nonRelDocs = new HashSet<String>();
						} else {
							nonRelDocs = nonRel.get(query);
						}
						
						nonRelDocs.add(docno);
						nonRel.put(query, nonRelDocs);
					}
				}
				
				if(! orderedQueryNames.contains(query))
					orderedQueryNames.add(query);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean isRel(String query, String docno) {
		if(!rel.containsKey(query)) {
			//System.err.println("no relevant documents found for query " + query);
			return false;
		}
		return rel.get(query).contains(docno);
	}
	
	public int getRelLevel(String query, String docno) {
		if (!relLevels.containsKey(query)) {
			return 0;
		}
		try {
			return relLevels.get(query).get(docno);
		} catch (NullPointerException e) {
			return 0;
		}
	}

	/**
	 * 
	 * @param query just the query title
	 * @return      a Set of docnos for relevant docs
	 */
	public Set<String> getRelDocs(String query) {
		if(!rel.containsKey(query)) {
			//System.err.println("no relevant documents found for query " + query);
			return null;
		}
		return rel.get(query);
	}

	public Set<String> getNonRelDocs(String query) {
		if(!nonRel.containsKey(query)) {
			//System.err.println("no non-relevant documents found for query " + query);
			return null;
		}
		return nonRel.get(query);
	}

	/**
	 * Needed if we want ALL documents (relevant and non) in the pool for this query.
	 * @param query
	 * @return
	 */
	public Set<String> getPool(String query) {
		if(!nonRel.containsKey(query) && !rel.containsKey(query)) {
			System.err.println("no judgments found for query " + query);
			return null;
		}
		Set<String> pool = new HashSet<String>();
		if(nonRel.containsKey(query))
			pool.addAll(nonRel.get(query));
		if(rel.containsKey(query))
			pool.addAll(rel.get(query));

		return pool;
	}

	public double numRel(String query) {
		if(!rel.containsKey(query)) {
			//System.err.println("no relevant documents found for query " + query);
			return 0.0;
		}
		return (double)rel.get(query).size();
	}

	public List<String> getOrderedQueryList() {
		return orderedQueryNames;
	}
	
	public void removeQrel(String queryName, String docno) {
		if(!rel.containsKey(queryName)) {
			System.err.println("can't remove docno " + docno + " from qrels for query " + queryName + " because the query has no qrels.");
			return;
		}
		rel.get(queryName).remove(docno);
	}
	public void addQrel(String queryName, String docno) {
		if(!rel.containsKey(queryName)) {
			Set<String> qrels = new HashSet<String>();
			rel.put(queryName, qrels);
		}
		rel.get(queryName).add(docno);
	}

	public boolean contains(String docno) {
	    for (String key: rel.keySet()) {
	        Set<String> val = rel.get(key);
	        if (val.contains(docno))
	            return true;
	    }
	    for (String key: nonRel.keySet()) {
	        Set<String> val = nonRel.get(key);
	        if (val.contains(docno))
	            return true;
	    }
	    return false;
	}
	
    public boolean contains(String docno, String query) {
        if ( (rel != null && rel.get(query) != null&& rel.get(query).contains(docno)) || 
                (nonRel != null && nonRel.get(query) != null && nonRel.get(query).contains(docno)) )
                return true;
        return false;
    }
}
