import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.hsqldb.lib.StringInputStream;
import org.junit.Test;

import com.esri.json.EsriFeature;
import com.esri.json.EsriJsonFactory;
import com.esri.json.hadoop.UnenclosedJsonInputFormat;
import com.esri.json.hadoop.UnenclosedJsonRecordReader;


public class TestUnenclosedJsonInputFormat {

	private UnenclosedJsonRecordReader getReaderFor(String resource) throws IOException {
		Path path = new Path(this.getClass().getResource(resource).getFile());

		JobConf conf = new JobConf();
		
		// setup input format for local resource
		FileInputFormat.setInputPaths(conf, path);
		UnenclosedJsonInputFormat format = new UnenclosedJsonInputFormat();
		InputSplit [] splits = format.getSplits(conf, 1);
		
		return (UnenclosedJsonRecordReader) format.getRecordReader(splits[0], conf, null);
	}
	
	@Test
	public void TestValidDocument() throws IOException {
		UnenclosedJsonRecordReader reader = getReaderFor("unenclosed.json");
		
		LongWritable key = reader.createKey();
		Text value = reader.createValue();
		
		// document has 3 records, reader.next should only return true 3 times
		assertTrue(reader.next(key, value));
		EsriFeature feature = EsriJsonFactory.FeatureFromJson(new StringInputStream(value.toString()));
		assertEquals(feature.attributes.get("NAME"), "Vermont");
		
		assertTrue(reader.next(key, value));
		feature = EsriJsonFactory.FeatureFromJson(new StringInputStream(value.toString()));
		assertEquals(feature.attributes.get("NAME"), "Minnesota");
		
		assertTrue(reader.next(key, value));
		feature = EsriJsonFactory.FeatureFromJson(new StringInputStream(value.toString()));
		assertEquals(feature.attributes.get("NAME"), "Oregon");
		
		assertFalse(reader.next(key, value));
	}
	
	@Test
	public void TestInvalidDocument() throws IOException {
		UnenclosedJsonRecordReader reader = getReaderFor("unenclosed.invalid.json");
		
		LongWritable key = reader.createKey();
		Text value = reader.createValue();
		
		assertTrue(reader.next(key, value)); // first record is ok
		assertFalse(reader.next(key, value)); // second record has an extra '{' and should fail
		
	}
}
