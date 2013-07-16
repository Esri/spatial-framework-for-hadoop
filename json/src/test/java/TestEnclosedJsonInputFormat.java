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
import com.esri.json.hadoop.EnclosedJsonInputFormat;
import com.esri.json.hadoop.EnclosedJsonRecordReader;


public class TestEnclosedJsonInputFormat {

	private EnclosedJsonRecordReader getReaderFor(String resource) throws IOException {
		Path path = new Path(this.getClass().getResource(resource).getFile());

		JobConf conf = new JobConf();
		
		// setup input format for local resource
		FileInputFormat.setInputPaths(conf, path);
		EnclosedJsonInputFormat format = new EnclosedJsonInputFormat();
		InputSplit [] splits = format.getSplits(conf, 1);
		
		return (EnclosedJsonRecordReader) format.getRecordReader(splits[0], conf, null);
	}
	
	@Test
	public void TestValidDocument() throws IOException {
		EnclosedJsonRecordReader reader = getReaderFor("enclosed.json");
		
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
}
