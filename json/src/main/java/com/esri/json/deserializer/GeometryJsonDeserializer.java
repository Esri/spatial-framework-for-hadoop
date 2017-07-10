package com.esri.json.deserializer;
import java.io.IOException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;

/**
 * 
 * Deserializes a JSON geometry definition into a Geometry instance
 */
public class GeometryJsonDeserializer extends JsonDeserializer<Geometry> {

	public GeometryJsonDeserializer(){}
	
	@Override
	public Geometry deserialize(JsonParser arg0, DeserializationContext arg1)
			throws IOException, JsonProcessingException {
		return GeometryEngine.jsonToGeometry(arg0).getGeometry();
	}
}
