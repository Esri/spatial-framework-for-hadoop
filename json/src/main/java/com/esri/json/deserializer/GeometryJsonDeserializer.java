package com.esri.json.deserializer;
import java.io.IOException;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;

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