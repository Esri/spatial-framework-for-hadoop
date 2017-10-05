package com.esri.json.serializer;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

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
