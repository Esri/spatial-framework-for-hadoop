package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
		name = "ST_AsShape",
		value = "_FUNC_(ST_Geometry) - return Esri shape representation of geometry\n",
		extended = "Example:\n" +
		"  SELECT _FUNC_(ST_Point(1, 2)) FROM onerow; -- Esri shape representation of POINT (1 2)\n"
		)
public class ST_AsShape extends ST_Geometry {

	static final Log LOG = LogFactory.getLog(ST_AsShape.class.getName());
	
	public BytesWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		try {
			// Get Esri shape representation
			Geometry esriGeometry = ogcGeometry.getEsriGeometry();
			byte[] esriShape = GeometryEngine.geometryToEsriShape(esriGeometry);
			return new BytesWritable(esriShape);
		} catch (Exception e){
			LOG.error(e.getMessage());
			return null;
		}
	}
}
