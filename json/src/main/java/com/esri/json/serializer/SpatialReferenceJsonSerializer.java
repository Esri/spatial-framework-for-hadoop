package com.esri.json.serializer;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import com.esri.core.geometry.SpatialReference;

public class SpatialReferenceJsonSerializer extends JsonSerializer<SpatialReference>{

	@Override
	public void serialize(SpatialReference spatialReference, JsonGenerator jsonGenerator,
			SerializerProvider arg2) throws IOException,
			JsonProcessingException {
			
		int wkid = spatialReference.getID();
		
		jsonGenerator.writeStartObject();
		jsonGenerator.writeObjectField("wkid", wkid);
		jsonGenerator.writeEndObject();
	}

}
