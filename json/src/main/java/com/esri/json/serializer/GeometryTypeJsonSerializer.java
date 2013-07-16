package com.esri.json.serializer;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.Type;

public class GeometryTypeJsonSerializer extends JsonSerializer<Geometry.Type>{

	@Override
	public void serialize(Type geometryType, JsonGenerator jsonGenerator, SerializerProvider arg2)
			throws IOException, JsonProcessingException {
		jsonGenerator.writeString("esriGeometry" + geometryType);
	}
}
