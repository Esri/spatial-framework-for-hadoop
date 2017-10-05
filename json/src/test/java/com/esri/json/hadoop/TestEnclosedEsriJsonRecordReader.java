package com.esri.json.hadoop;

import static org.junit.Assert.*;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TestEnclosedEsriJsonRecordReader {

	private TaskAttemptContext createTaskAttemptContext(Configuration conf, TaskAttemptID taid)
		throws Exception  {       //shim
		try {                     // Hadoop-1
			return (TaskAttemptContext)TaskAttemptContext.class.
				getConstructor(Configuration.class, TaskAttemptID.class).
				newInstance(conf, taid);
		} catch (Exception e) {   // Hadoop-2
			Class<?> clazz =
				Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
			return (TaskAttemptContext)clazz.getConstructor(Configuration.class, TaskAttemptID.class).
				newInstance(conf, taid);
		}
	}

	long [] getRecordIndexesInFile(String resource, int start, int end) throws Exception {
		EnclosedEsriJsonRecordReader reader = new EnclosedEsriJsonRecordReader();
		Path path = new Path(this.getClass().getResource(resource).getFile());
		FileSplit split = new FileSplit(path, start, end - start, new String[0]);
        try {
			TaskAttemptContext tac =
                createTaskAttemptContext(new Configuration(), new TaskAttemptID());
			reader.initialize(split, tac);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
		List<Long> linesList = new LinkedList<Long>();
		
		LongWritable key = null;
		//Text value = null;
		
		try {
			while (reader.nextKeyValue()) {
				key = reader.getCurrentKey();
				//value = reader.getCurrentValue();
				linesList.add(key.get());
				//System.out.println(key.get() + " - " + value);
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
		
		long [] offsets = new long[linesList.size()];  // list-as-array
		for (int i=0;i<linesList.size();i++) {
			offsets[i] = linesList.get(i);
		}
        reader.close();
		return offsets;
	}

	@Test
	public void TestArbitrarySplitLocations() throws Exception {
		//long [] recordBreaks = new long[] { 1872, 11284, 0, 0, 0,  };
		//assertArrayEquals(new long[] { 94L }, getRecordIndexesInFile("sample-study-area.json", 0, 208));
		long[] rslt = getRecordIndexesInFile("sample-study-area.json", 0, 208);
		assertEquals(1, rslt.length);
	}

	@Test
	public void TestMrv1() throws Exception {
		//long [] recordBreaks = new long[] { 1872, 11284, 0, 0, 0,  };
		//assertArrayEquals(new long[] { 94L }, getRecordIndexesInFile("sample-study-area.json", 0, 208));
		Path path = new Path(this.getClass().getResource("sample-study-area.json").getFile());
		org.apache.hadoop.mapred.JobConf conf = new org.apache.hadoop.mapred.JobConf();
		org.apache.hadoop.mapred.FileSplit split =
			new org.apache.hadoop.mapred.FileSplit(path, 0, 208, new String[0]);
		EnclosedEsriJsonRecordReader reader = new EnclosedEsriJsonRecordReader(split, conf);
		LongWritable key = reader.createKey();
		Text value = reader.createValue();
		assertTrue (reader.next(key, value));
		//System.out.println(key.get() + " - " + value.toString());
		assertFalse (reader.next(key, value));
		reader.close();
	}
    /* *  Obsolete
	@Test
	public void TestLegacy() throws Exception {
		Path path = new Path(this.getClass().getResource("sample-study-area.json").getFile());
		org.apache.hadoop.mapred.JobConf conf = new org.apache.hadoop.mapred.JobConf();
		org.apache.hadoop.mapred.FileSplit split =
			new org.apache.hadoop.mapred.FileSplit(path, 0, 208, new String[0]);
		EnclosedEsriJsonRecordReader reader = new EnclosedJsonRecordReader(split, conf);
		LongWritable key = reader.createKey();
		Text value = reader.createValue();
		assertTrue (reader.next(key, value));
		//System.out.println(key.get() + " - " + value.toString());
		assertFalse (reader.next(key, value));
		reader.close();
	}
    * */
}
