package com.esri.json;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.JsonMappingException;

import com.esri.core.geometry.Geometry;


public class EsriFeature {
	/**
	 * Map of attributes
	 */
	public Map<String, Object> attributes;
	
	/**
	 * Geometry associated with this feature
	 */
	public Geometry geometry;
	
	public String toJson() throws JsonGenerationException, JsonMappingException, IOException{
		return EsriJsonFactory.JsonFromFeature(this);
	}
	
	/**
	 * @param jsonStream JSON input stream
	 * @return EsriFeature instance that describes the fully parsed JSON representation
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public static EsriFeature fromJson(InputStream jsonStream) throws JsonParseException, IOException
	{
		return EsriJsonFactory.FeatureFromJson(jsonStream);
	}
	
	/**
	 * 
	 * @param JsonParser parser that is pointed at the root of the JSON file created by ArcGIS
	 * @return EsriFeature instance that describes the fully parsed JSON representation
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public static EsriFeature fromJson(JsonParser parser) throws JsonParseException, IOException
	{	
		return EsriJsonFactory.FeatureFromJson(parser);
	}
}
