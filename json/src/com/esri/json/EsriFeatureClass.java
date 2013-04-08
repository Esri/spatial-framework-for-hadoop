package com.esri.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.SpatialReference;


@JsonIgnoreProperties(ignoreUnknown = true)
public class EsriFeatureClass {
	public String displayFieldName;
	
	/**
	 * Map of field aliases for applicable fields in this feature class
	 */
	public Map<String, Object> fieldAliases;
	
	/**
	 * Esri geometry type (Polygon, Point, ...)
	 */
	public Geometry.Type geometryType;
	
	/**
	 * Spatial reference for the feature class (null, if undefined)
	 */
	public SpatialReference spatialReference;
	
	/**
	 * Array of field definitions (name, type, alias, ...)
	 */
	public EsriField [] fields;
	
	/**
	 * Array of features (attributes, geometry)
	 */
	public EsriFeature [] features;
	
	
	/**
	 * 
	 * @return JSON string representation of this feature class
	 * @throws JsonGenerationException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public String toJson() throws JsonGenerationException, JsonMappingException, IOException{
		return EsriJsonFactory.JsonFromFeatureClass(this);
	}
	
	/**
	 * 
	 * @param jsonStream JSON input stream
	 * @return EsriFeatureClass instance that describes the fully parsed JSON representation
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public static EsriFeatureClass fromJson(InputStream jsonStream) throws JsonParseException, IOException
	{
		return EsriJsonFactory.FeatureClassFromJson(jsonStream);
	}

	/**
	 * 
	 * @param JsonParser parser that is pointed at the root of the JSON file created by ArcGIS
	 * @return EsriFeatureClass instance that describes the fully parsed JSON representation
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public static EsriFeatureClass fromJson(JsonParser parser) throws JsonParseException, IOException
	{	
		return EsriJsonFactory.FeatureClassFromJson(parser);
	}
}