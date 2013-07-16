package com.esri.json.serializer;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

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
