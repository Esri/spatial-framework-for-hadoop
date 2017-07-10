package com.esri.json.serializer;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.Type;

public class GeometryTypeJsonSerializer extends JsonSerializer<Geometry.Type>{

	@Override
	public void serialize(Type geometryType, JsonGenerator jsonGenerator, SerializerProvider arg2)
			throws IOException, JsonProcessingException {
		jsonGenerator.writeString("esriGeometry" + geometryType);
	}
}
