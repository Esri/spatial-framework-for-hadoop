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
	value = "_FUNC_(polygon) - returns the point that is the center of the polygon's envelope",
	extended = "Example:\n"
	+ "  > SELECT _FUNC_(ST_GeomFromText('polygon ((0 0, 3 6, 6 0, 0 0))')) FROM src LIMIT 1;  -- POINT(3 3)\n"
	+ "  > SELECT _FUNC_(ST_GeomFromText('polygon ((0 0, 0 8, 8 0, 0 0))')) FROM src LIMIT 1;  -- POINT(4 4)\n"
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

		GeometryUtils.OGCType ogcType = GeometryUtils.getType(geomref);
		switch(ogcType) {
		case ST_MULTIPOLYGON:
		case ST_POLYGON:
			int wkid = GeometryUtils.getWKID(geomref);
			SpatialReference spatialReference = null;
			if (wkid != GeometryUtils.WKID_UNKNOWN) {
				spatialReference = SpatialReference.create(wkid);
			}
			Envelope envBound = new Envelope();
			ogcGeometry.getEsriGeometry().queryEnvelope(envBound);
			Point centroid = new Point((envBound.getXMin() + envBound.getXMax()) / 2.,
									   (envBound.getYMin() + envBound.getYMax()) / 2.);
			return GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(centroid,
																  spatialReference));
		default:
			LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POLYGON, ogcType);
			return null;
		}
	}

}
