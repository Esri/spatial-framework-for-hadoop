package com.esri.json.hadoop;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Record reader for reading features from a feature exported as JSON in Esri standard format.
 * 
 * Each record returned is a string { "attributes" : [...], "geometry" : ... }
 *
 */
public class EnclosedEsriJsonRecordReader implements RecordReader<LongWritable, Text>{

	private InputStream inputStream;
	private FileSplit fileSplit;
	private JsonParser parser;
	
	public EnclosedEsriJsonRecordReader(InputSplit split, Configuration conf) throws IOException
	{
		fileSplit = (FileSplit)split;
		
		Path filePath = fileSplit.getPath();
		
		FileSystem fs = filePath.getFileSystem(conf);
		
		inputStream = fs.open(filePath);
	}

	@Override
	public void close() throws IOException {
		if (inputStream != null)
			inputStream.close();
	}


	@Override
	public LongWritable createKey() {
		return new LongWritable();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public long getPos() throws IOException {
		if (parser == null){
			return 0;
		} else {
			return parser.getCurrentLocation().getCharOffset();
		}
	}

	@Override
	public boolean next(LongWritable key, Text value) throws IOException {
		JsonToken token;
		
		// first call to nextKeyValue() so we need to create the parser and move to the
		// feature array
		if (parser == null){
			parser = new JsonFactory().createJsonParser(inputStream);
			
			parser.setCodec(new ObjectMapper());
			
			token = parser.nextToken();
			
			while (token != null && !(token == JsonToken.START_ARRAY &&
					parser.getCurrentName() != null && parser.getCurrentName().equals("features"))) {
				token = parser.nextToken();
			}
			
			if (token == null) return false; // never found the features array
		}

		key.set(parser.getCurrentLocation().getCharOffset());
		
		token = parser.nextToken();
		
		// this token should be a start object with no name
		if (token == null || !(token == JsonToken.START_OBJECT && parser.getCurrentName() == null))
			return false;
		
		
		JsonNode node = parser.readValueAsTree();
		
		value.set(node.toString());

		return true;
	}

	@Override
	public float getProgress() throws IOException {
		if (fileSplit.getLength() == 0 || parser == null) return 0;
		
		return (float)parser.getCurrentLocation().getByteOffset() / fileSplit.getLength();
	}
}

