package edu.gslis.hadoop.trec;

import java.util.regex.Pattern;

import edu.umd.cloud9.collection.trec.TrecDocument;

public class TrecUtils {
    
    private final static Pattern
    headerPat = Pattern.compile("^(.*?)<",  
        Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
    scriptPat = Pattern.compile("(?s)<script(.*?)</script>", 
        Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
    tagsPat = Pattern.compile("<[^>]+>", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL),
    spacePat = Pattern.compile("[ \n\r\t]+", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);

    
    public static String getTextFromHtml(String html) {
        html = headerPat.matcher(html).replaceFirst("<");
        html = scriptPat.matcher(html).replaceAll(" ");
        html = tagsPat.matcher(html).replaceAll(" ");
        html = spacePat.matcher(html).replaceAll(" ");
        html = html.toLowerCase();
        
        return html;            
    }   
    
	/**
	 * Get the text element
	 */
	public static String getText(TrecDocument doc) {

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
	
	/**
	 * Get the document number
	 */
	   public static String getDocNo(TrecDocument doc) {

	        String text = "";
	        String content = doc.getContent();
	        int start = content.indexOf("<DOCNO>");
	        if (start == -1) {
	            text = "";
	        } else {
	            int end = content.indexOf("</DOCNO>", start);
	            text= content.substring(start + 7, end).trim();
	        }
	        return text;
	    }
}
