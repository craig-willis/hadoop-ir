package edu.gslis.hbase.trec;


public class SearchResult  implements Comparable<SearchResult> {
    String docid;
    double score;
    
    public SearchResult(String docid, double score) {
        this.docid = docid;
        this.score = score;
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
}
