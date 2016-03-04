package com.esri.hadoop.hive.serde;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonParser;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.ogc.OGCGeometry;


public class EsriJsonSerDe extends BaseJsonSerDe {

	static final Log LOG = LogFactory.getLog(EsriJsonSerDe.class.getName());

	@Override
	protected String outGeom(OGCGeometry geom) {
		return geom.asJson();
	}

	@Override
	protected OGCGeometry parseGeom(JsonParser parser) {
		MapGeometry mapGeom = GeometryEngine.jsonToGeometry(parser);
		return OGCGeometry.createFromEsriGeometry(mapGeom.getGeometry(), mapGeom.getSpatialReference());
	}
}
