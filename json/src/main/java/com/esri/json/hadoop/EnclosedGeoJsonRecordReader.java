package com.esri.json.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;

/**
 * Record reader for reading features from GeoJSON FeatureCollection.
 * 
 * Each record returned is a string { "type" : [...], "properties" : [...], "geometry" : ... }
 */
public class EnclosedGeoJsonRecordReader extends EnclosedBaseJsonRecordReader {

	public EnclosedGeoJsonRecordReader() throws IOException {  // explicit just to declare exception
        super();
	}

	public EnclosedGeoJsonRecordReader(InputSplit split, Configuration conf) throws IOException
	{
        super(split, conf);
	}

}
