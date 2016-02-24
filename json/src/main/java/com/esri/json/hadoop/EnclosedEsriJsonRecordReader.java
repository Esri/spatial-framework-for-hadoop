package com.esri.json.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;

/**
 * Record reader for reading features from a feature exported as JSON in Esri standard format.
 * 
 * Each record returned is a string { "attributes" : [...], "geometry" : ... }
 *
 */
public class EnclosedEsriJsonRecordReader extends EnclosedBaseJsonRecordReader {

	public EnclosedEsriJsonRecordReader() throws IOException {  // explicit just to declare exception
        super();
	}
	
	public EnclosedEsriJsonRecordReader(InputSplit split, Configuration conf) throws IOException
	{
        super(split, conf);
	}

}
