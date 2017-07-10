package com.esri.json;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.SpatialReference;
import com.esri.json.deserializer.GeometryJsonDeserializer;
import com.esri.json.deserializer.GeometryTypeJsonDeserializer;
import com.esri.json.deserializer.SpatialReferenceJsonDeserializer;
import com.esri.json.serializer.GeometryJsonSerializer;
import com.esri.json.serializer.GeometryTypeJsonSerializer;
import com.esri.json.serializer.SpatialReferenceJsonSerializer;

public class EsriJsonFactory {

	private static final ObjectMapper jsonObjectMapper;
	private static final JsonFactory jsonFactory = new JsonFactory();
	
	static {
		jsonObjectMapper = new ObjectMapper();

		SimpleModule module = new SimpleModule("EsriJsonModule", new Version(1, 0, 0, null));
		
		// add deserializers and serializers for types that can't be mapped field for field from the JSON
		module.addDeserializer(Geometry.class, new GeometryJsonDeserializer());
		module.addDeserializer(SpatialReference.class, new SpatialReferenceJsonDeserializer());
		module.addDeserializer(Geometry.Type.class, new GeometryTypeJsonDeserializer());
		
		module.addSerializer(Geometry.class, new GeometryJsonSerializer());
		module.addSerializer(Geometry.Type.class, new GeometryTypeJsonSerializer());
		module.addSerializer(SpatialReference.class, new SpatialReferenceJsonSerializer());
		
		jsonObjectMapper.registerModule(module);
	}
	
	
	private EsriJsonFactory(){ /* disable instance creation */ }
	
	/**
	 * Create JSON from an {@link com.esri.json.EsriFeatureClass}
	 * 
	 * @param featureClass feature class to convert to JSON
	 * @return JSON string representing the given feature class
	 * @throws JsonGenerationException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public static String JsonFromFeatureClass(EsriFeatureClass featureClass) throws JsonGenerationException, JsonMappingException, IOException{
		return jsonObjectMapper.writeValueAsString(featureClass);
	}
	
	/**
	 * Construct an {@link com.esri.json.EsriFeatureClass} from JSON
	 * 
	 * @param jsonInputStream JSON input stream
	 * @return EsriFeatureClass instance that describes the fully parsed JSON representation
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public static EsriFeatureClass FeatureClassFromJson(InputStream jsonInputStream) throws JsonParseException, IOException{
		JsonParser parser = jsonFactory.createJsonParser(jsonInputStream);
		return FeatureClassFromJson(parser);
	}
	
	/**
	 * Construct an {@link com.esri.json.EsriFeatureClass} from JSON
	 * 
	 * @param JsonParser parser that is pointed at the root of the JSON file created by ArcGIS
	 * @return EsriFeatureClass instance that describes the fully parsed JSON representation
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public static EsriFeatureClass FeatureClassFromJson(JsonParser parser) throws JsonProcessingException, IOException{
		parser.setCodec(jsonObjectMapper);
		return parser.readValueAs(EsriFeatureClass.class);
	}
	
	
	/**
	 * Create JSON from an {@link com.esri.json.EsriFeature}
	 * 
	 * @param feature feature to convert to JSON
	 * @return JSON string representing the given feature
	 * @throws JsonGenerationException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public static String JsonFromFeature(EsriFeature feature) throws JsonGenerationException, JsonMappingException, IOException{
		return jsonObjectMapper.writeValueAsString(feature);
	}
	
	/**
	 * Construct an {@link com.esri.json.EsriFeature} from JSON
	 * 
	 * @param jsonInputStream JSON input stream
	 * @return EsriFeature instance that describes the fully parsed JSON representation
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public static EsriFeature FeatureFromJson(InputStream jsonInputStream) throws JsonParseException, IOException{
		JsonParser parser = jsonFactory.createJsonParser(jsonInputStream);
		return FeatureFromJson(parser);
	}
	
	/**
	 * Construct an {@link com.esri.json.EsriFeature} from JSON
	 * 
	 * @param JsonParser parser that is pointed at the root of the JSON file created by ArcGIS
	 * @return EsriFeature instance that describes the fully parsed JSON representation
	 * @throws JsonParseException
	 * @throws IOException
	 */
	public static EsriFeature FeatureFromJson(JsonParser parser) throws JsonProcessingException, IOException{
		parser.setCodec(jsonObjectMapper);
		return parser.readValueAs(EsriFeature.class);
	}
	
}
