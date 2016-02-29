package com.esri.hadoop.hive.serde;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;

import com.esri.core.geometry.ogc.OGCGeometry;


public class GeoJsonSerDe extends BaseJsonSerDe {

	static final Log LOG = LogFactory.getLog(GeoJsonSerDe.class.getName());

    ObjectMapper mapper = null;

	public GeoJsonSerDe() {
		super();
		attrLabel = "properties";
		mapper = new ObjectMapper();
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector oi)
			throws SerDeException {
		StringWriter writer = new StringWriter();
		return new Text(writer.toString());
	}

	@Override
	protected OGCGeometry parseGeom(JsonParser parser) {
		try {
			JsonNode node = mapper.readTree(parser);
			return OGCGeometry.fromGeoJson(node.toString());
		} catch (JsonProcessingException e1) {
			e1.printStackTrace();			// TODO Auto-generated catch block
		} catch (IOException e1) {
			e1.printStackTrace();			// TODO Auto-generated catch block
		} catch (JSONException e) {
			e.printStackTrace();			// TODO Auto-generated catch block
		}
		return null;  // ?
	}
}
