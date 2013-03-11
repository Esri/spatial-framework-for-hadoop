package com.esri.json.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

public class UnenclosedJsonInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public org.apache.hadoop.mapred.RecordReader<LongWritable, Text> getRecordReader(
			org.apache.hadoop.mapred.InputSplit arg0, JobConf arg1,
			Reporter arg2) throws IOException {
		return new UnenclosedJsonRecordReader(arg0, arg1);
	}
}
