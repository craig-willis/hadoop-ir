package edu.gslis.hbase.trec;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class Eval {
    
    private Map<String,List<Double>> aggregateStats;

    List<SearchResult> results = null;
    Qrels qrels = null;
    public Eval(List<SearchResult> results, Qrels qrels) {
        this.results = results;
        this.qrels = qrels;
        this.aggregateStats = new HashMap<String,List<Double>>();
    }

    
    public double precision(String queryName) {
        double numRelRet = this.numRelRet(queryName);
        if(numRelRet==0.0) {
            return 0.0;
        }            
        double numRet    = (double)results.size();
        double p = numRelRet / numRet;
        return p;            
    }
    

    public double precisionAt(String queryName, int k) {
        double relRet = 0;
        
        if(results == null)
            return 0.0;
        
        for (int i=0; i<k; i++) {
            if (qrels.isRel(queryName, results.get(i).getDocid()))
                relRet ++;
        }
        
        double p = relRet / (double)k;
        return p;
    }
    
    public double recall(String queryName) {
        double numRelRet = this.numRelRet(queryName);
        if(numRelRet==0.0) {
            return 0.0;
        }
        
        double numRel = qrels.numRel(queryName);        
        return numRelRet / numRel;
        
    }
    
    public double numRelRet(String queryName) {
        double relRet  = 0.0;
        
        if(results == null)
            return 0.0;
        
        for (SearchResult result: results) {
            if(qrels.isRel(queryName, result.getDocid())) {
                relRet += 1.0;
            }
        }

        return (double)relRet;
    }
    
    public double numRet(String queryName) {
        if(results == null)
            return 0.0;
        
        return (double)results.size();
    }
    
    public double f1Query(String queryName) {

        
        double precision = this.precision(queryName);
        double recall    = this.recall(queryName);
        
        double f = 2 * (precision * recall) / (precision + recall);
        
        //System.err.println("EVAL: " + results.size() + " " + f);

        if(Double.isNaN(f)) {
            return 0.0;
        }
        return f;
    }
    
    public double f1Query(String queryName, double beta) throws Exception {

        beta *= beta;
        
        double precision = this.precision(queryName);
        double recall    = this.recall(queryName);
        
        double f =  (1 + beta) * (precision * recall) / (beta * precision + recall);
        
        if(Double.isNaN(f)) {
            return 0.0;
        }
        return f;
    }
    

    public double utility(String queryName, double truePosWeight) {
        if(results == null)
            return 0.0;
        
        double truePos  = this.numRelRet(queryName);
        double falsePos = (double)results.size() - truePos;
        
        return Math.max(-100, truePosWeight * truePos - falsePos);
        
    }
    
    public void addStat(String statName, double value) {
        List<Double> stats = null;
        if(aggregateStats.containsKey(statName)) {
            stats = aggregateStats.get(statName);
        } else {
            stats = new LinkedList<Double>();
        }
        stats.add(value);
        
        aggregateStats.put(statName, stats);
    }
    
    public double avg(String statName) {
        if(! aggregateStats.containsKey(statName)) {
            return -1.0;
        }
        
        double mean = 0.0;

        Iterator<Double> statIterator = aggregateStats.get(statName).iterator();
        while(statIterator.hasNext()) {
            mean += statIterator.next();
        }
        
        mean /= (double)aggregateStats.get(statName).size();
        
        return mean;
    }
    
    public double map(String queryName) {
        double ap = 0.0;
        if (results == null) 
            return 0.0;
        
        int numRel = qrels.getRelDocs(queryName).size();
        
        int numRelRet = 0;
        for (int i = 0; i < results.size(); i++) {
            SearchResult result = results.get(i);
            if (qrels.isRel(queryName, result.getDocid())) {
                numRelRet++;
                ap += numRelRet/(double)(i+1);
            }
        }
       
        ap /= numRel;
        
        return ap;
    }
    
    public double avgPrecision(String queryName) {
        
        double avgPrecision  = 0.0;
        
        if(results == null)
            return 0.0;
        
        int k = 1;
        int numRelRet = 0;
        for (SearchResult result: results) {
            if(qrels.isRel(queryName, result.getDocid())) {
                numRelRet++;
                avgPrecision += (double)numRelRet/k;
            }
            k++;
        }
        avgPrecision /= numRet(queryName);
        
        return avgPrecision;
    }
}
