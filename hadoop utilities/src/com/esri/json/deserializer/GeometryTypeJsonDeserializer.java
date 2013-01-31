package com.esri.json.deserializer;

import java.io.IOException;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;

import com.esri.core.geometry.Geometry;

/**
 * 
 * Deserializes a JSON geometry type enumeration into a Geometry.Type.* enumeration
 */
public class GeometryTypeJsonDeserializer extends JsonDeserializer<Geometry.Type> {

	public GeometryTypeJsonDeserializer(){}
	
	@Override
	public Geometry.Type deserialize(JsonParser parser, DeserializationContext arg1)
			throws IOException, JsonProcessingException {
		
		String type_text = parser.getText();
		
		// geometry type enumerations coming from the JSON are prepended with "esriGeometry" (i.e. esriGeometryPolygon)
		// while the geometry-java-api uses the form Geometry.Type.Polygon
		if (type_text.startsWith("esriGeometry"))
		{
			// cut out esriGeometry to match Geometry.Type enumeration values
			type_text = type_text.substring(12);
			
			try {
				return Enum.valueOf(Geometry.Type.class, type_text);
			} catch (Exception e){
				// parsing failed, fall through to unknown geometry type
			}
		}
		
		
		return Geometry.Type.Unknown;
	}
}