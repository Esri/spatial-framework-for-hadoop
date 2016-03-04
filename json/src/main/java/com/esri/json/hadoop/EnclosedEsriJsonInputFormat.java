package com.esri.json.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * FileInputFormat for reading features from a feature exported as JSON in Esri standard format.
 */
public class EnclosedEsriJsonInputFormat extends FileInputFormat<LongWritable, Text>
    implements org.apache.hadoop.mapred.InputFormat<LongWritable,Text> {

	// Mrv1 implementation member will be used only for getSplits(), and
    // will be instantiated only when Mrv1 in use.
	private org.apache.hadoop.mapred.FileInputFormat<LongWritable,Text> ifmtMrv1 = null;

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {  // MRv2
		return new EnclosedEsriJsonRecordReader();
	}

	@Override
	public org.apache.hadoop.mapred.RecordReader<LongWritable, Text> getRecordReader(  // MRv1
			org.apache.hadoop.mapred.InputSplit arg0,
			org.apache.hadoop.mapred.JobConf arg1,
			org.apache.hadoop.mapred.Reporter arg2) throws IOException {
		return new EnclosedEsriJsonRecordReader(arg0, arg1);
	}

	@Override
	public org.apache.hadoop.mapred.InputSplit[] getSplits(  // MRv1
			org.apache.hadoop.mapred.JobConf arg0,
			int arg1) throws IOException {
		ifmtMrv1 = (ifmtMrv1!=null) ? ifmtMrv1 :
		  new org.apache.hadoop.mapred.FileInputFormat<LongWritable,Text>() {
			@Override
			public boolean isSplitable(FileSystem fs, Path filename) {
				return false;
			}
            // Dummy method to satisfy interface but not meant to be called
			public org.apache.hadoop.mapred.RecordReader<LongWritable, Text> getRecordReader(
			  org.apache.hadoop.mapred.InputSplit ign0,
			  org.apache.hadoop.mapred.JobConf ign1,
			  org.apache.hadoop.mapred.Reporter ign2) throws IOException {
				throw new UnsupportedOperationException("not meant to be called");
			}
		  };
		return ifmtMrv1.getSplits(arg0, arg1);
	}

	@Override
	public boolean isSplitable(JobContext jc, Path filename) {
		return false;
	}
}
