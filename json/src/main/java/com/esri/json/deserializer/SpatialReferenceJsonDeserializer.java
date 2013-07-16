package com.esri.json.deserializer;

import java.io.IOException;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;

import com.esri.core.geometry.SpatialReference;

/**
 * 
 * Deserializes a JSON spatial reference definition into a SpatialReference instance
 */
public class SpatialReferenceJsonDeserializer extends JsonDeserializer<SpatialReference> {

	public SpatialReferenceJsonDeserializer(){}
	
	@Override
	public SpatialReference deserialize(JsonParser parser, DeserializationContext arg1)
			throws IOException, JsonProcessingException {
		try {
			return SpatialReference.fromJson(parser);
		} catch (Exception e) {
			return null;
		}
	}
}
