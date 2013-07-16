package com.esri.json.serializer;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;

public class GeometryJsonSerializer extends JsonSerializer<Geometry> {

	@Override
	public void serialize(Geometry geometry, JsonGenerator jsonGenerator,
			SerializerProvider arg2) throws IOException,
			JsonProcessingException {
		
		jsonGenerator.writeRawValue(GeometryEngine.geometryToJson(null, geometry));
	}
}
