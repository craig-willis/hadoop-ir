package edu.gslis.hadoop.trec;

import org.apache.hadoop.mapred.FileInputFormat;



import edu.umd.cloud9.collection.Indexable;

/**
 * Abstract class representing a {@link FileInputFormat} for {@link Indexable} objects ({@code
 * org.apache.hadoop.mapreduce} API).
 */
public abstract class IndexableFileInputFormat<K, V extends Indexable> extends
    FileInputFormat<K, V> {
}
