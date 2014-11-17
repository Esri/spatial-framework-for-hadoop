package com.esri.json.hadoop;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Assert;
import org.junit.Test;

import com.esri.json.hadoop.UnenclosedJsonRecordReader;

public class TestUnenclosedJsonRecordReader {
	private UnenclosedJsonRecordReader getReaderFor(String resource, int start, int end) throws IOException {
		Path path = new Path(this.getClass().getResource(resource).getFile());
		
		JobConf conf = new JobConf();
		
		FileSplit split = new FileSplit(path, start, end - start, new String[0]);
		
		return new UnenclosedJsonRecordReader(split, conf);
	}

	int [] getRecordIndexesInReader(UnenclosedJsonRecordReader reader) throws IOException {
		List<Integer> linesList = new LinkedList<Integer>();
		
		LongWritable key = reader.createKey();
		Text value = reader.createValue();
		
		while (reader.next(key, value)) {
			int line = value.toString().charAt(23) - '0';
			linesList.add(line);
			System.out.println(key.get() + " - " + value.toString());
		}
		
		int [] lines = new int[linesList.size()];
		for (int i=0;i<linesList.size();i++) {
			lines[i] = linesList.get(i);
		}
		return lines;
	}
	
	@Test
	public void TestArbitrarySplitLocations() throws IOException {
		
		//int totalSize = 415;
		
		//int [] recordBreaks = new int[] { 0, 40, 80, 120, 160, 200, 240, 280, 320, 372 };
	
		
		Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 0, 40)));
		Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 0, 41)));
		Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 0, 42)));
		Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 39, 123)));
		
		Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 20, 123)));
		Assert.assertArrayEquals(new int[] { 1, 2, 3 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 40, 123)));
		Assert.assertArrayEquals(new int[] { 2, 3 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 41, 123)));
		Assert.assertArrayEquals(new int[] { 6, 7, 8 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 240, 340)));
		Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 353, 415)));
		Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 354, 415)));
		Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 355, 415)));

		Assert.assertArrayEquals(new int[] { 0, 1 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 0, 63)));
		Assert.assertArrayEquals(new int[] { 2, 3 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 63, 121)));
		Assert.assertArrayEquals(new int[] { 4 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 121, 187)));
		Assert.assertArrayEquals(new int[] { 5, 6 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 187, 264)));
		Assert.assertArrayEquals(new int[] { 7, 8 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 264, 352)));
		Assert.assertArrayEquals(new int[] { 9 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 352, 412)));

		Assert.assertArrayEquals(new int[] { 0 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 0, 23)));
		Assert.assertArrayEquals(new int[] { 1 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 23, 41)));
		// Assert.assertArrayEquals(new int[] { 2, 3 }, getRecordIndexesInReader(getReaderFor("unenclosed-json-simple.json", 41, 123)));
	}

}
