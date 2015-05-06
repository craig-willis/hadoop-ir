package edu.gslis.hbase.trec;


public class SearchResult  implements Comparable<SearchResult> {
    String docid;
    double score;
    long epoch = -1;

    public SearchResult(String docid, double score) {
        this.docid = docid;
        this.score = score;
    }

    public SearchResult(String docid, double score, long epoch) {
        this.docid = docid;
        this.score = score;
        this.epoch = epoch;
    }

    @Override
    public int compareTo(SearchResult o1) {        
        if (o1.score == score) {
            return o1.docid.compareTo(docid);
        }
        else
            return Double.compare(o1.score, score);        
    }
    
    public String getDocid() {
        return docid;
    }
    public double getScore() {
        return score;
    }
    public long getEpoch() {
        return epoch;
    }
}
