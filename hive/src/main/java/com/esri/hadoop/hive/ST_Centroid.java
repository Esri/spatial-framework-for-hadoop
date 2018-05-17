package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_Centroid",
	value = "_FUNC_(geometry) - returns the centroid of the geometry",
	extended = "Example:\n"
	+ "  > SELECT _FUNC_(ST_GeomFromText('point (2 3)'));  -- POINT(2 3)\n"
	+ "  > SELECT _FUNC_(ST_GeomFromText('multipoint ((0 0), (1 1), (1 -1), (6 0))'));  -- POINT(2 0)\n"
	+ "  > SELECT _FUNC_(ST_GeomFromText('linestring ((0 0, 6 0))'));  -- POINT(3 0)\n"
	+ "  > SELECT _FUNC_(ST_GeomFromText('linestring ((0 0, 2 4, 6 8))'));  -- POINT(3 4)\n"
	+ "  > SELECT _FUNC_(ST_GeomFromText('polygon ((0 0, 0 8, 8 8, 8 0, 0 0))'));  -- POINT(4 4)\n"
	+ "  > SELECT _FUNC_(ST_GeomFromText('polygon ((1 1, 5 1, 3 4))'));  -- POINT(3 2)\n"
	)

public class ST_Centroid extends ST_GeometryAccessor {
	static final Log LOG = LogFactory.getLog(ST_PointN.class.getName());

	public BytesWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		return GeometryUtils.geometryToEsriShapeBytesWritable(ogcGeometry.centroid());
	}

}
