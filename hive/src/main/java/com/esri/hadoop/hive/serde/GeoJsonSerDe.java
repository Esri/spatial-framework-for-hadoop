package com.esri.hadoop.hive.serde;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
	protected String outGeom(OGCGeometry geom) {
		return geom.asGeoJson();
	}

	@Override
	protected OGCGeometry parseGeom(JsonParser parser) {
		try {
			ObjectNode node = mapper.readTree(parser);
			return OGCGeometry.fromGeoJson(node.toString());
		} catch (JsonProcessingException e1) {
			e1.printStackTrace();			// TODO Auto-generated catch block
		} catch (IOException e1) {
			e1.printStackTrace();			// TODO Auto-generated catch block
		}
		return null;  // ?
	}
}
