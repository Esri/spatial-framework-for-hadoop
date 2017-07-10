package com.esri.json.deserializer;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

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
