package com.esri.json;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.deserializer.*;

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
	 * @param jsonStream JSON input stream
	 * @return EsriFeatureClass instance that describes the fully parsed JSON representation
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public static EsriFeatureClass fromJson(InputStream jsonStream) throws JsonParseException, IOException
	{
		JsonFactory factory = new JsonFactory();
		JsonParser parser = factory.createJsonParser(jsonStream);
		
		return fromJson(parser);
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
		ObjectMapper mapper = new ObjectMapper();
		parser.setCodec(mapper);
		
		SimpleModule module = new SimpleModule("EsriDeserializers", new Version(1, 0, 0, null));
		
		// add deserializers for types that can't be mapped field for field from the JSON
		module.addDeserializer(Geometry.class, new GeometryJsonDeserializer());
		module.addDeserializer(SpatialReference.class, new SpatialReferenceJsonDeserializer());
		module.addDeserializer(Geometry.Type.class, new GeometryTypeJsonDeserializer());
		
		mapper.registerModule(module);
		
		return parser.readValueAs(EsriFeatureClass.class);
	}
}