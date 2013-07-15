package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCPolygon;

@Description(
	name = "ST_InteriorRingN",
	value = "_FUNC_(ST_Polygon, n) - return ST_LineString which is the nth interior ring of the ST_Polygon (1-based index)",
	extended = "Example:\n"
	+ "  SELECT _FUNC_(ST_Polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'), 1) FROM src LIMIT 1;  -- LINESTRING (1 1, 5 1, 1 5, 1 1)\n"
	)
@HivePdkUnitTests(
	cases = {
		@HivePdkUnitTest(
			query = "select ST_Equals(ST_InteriorRingN(ST_Polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'), 1), ST_LineString('linestring(1 1, 5 1, 1 5, 1 1)')) from onerow",
			result = "true"
			),
		@HivePdkUnitTest(
			query = "select ST_InteriorRingN(null, 1) from onerow",
			result = "null"
			)
	}
)

public class ST_InteriorRingN extends ST_GeometryProcessing {
	static final Log LOG = LogFactory.getLog(ST_InteriorRingN.class.getName());

	public BytesWritable evaluate(BytesWritable geomref, IntWritable index) {
		if (geomref == null || geomref.getLength() == 0 || index == null) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		int idx = index.get() - 1;  // 1-based UI, 0-based engine
		if (GeometryUtils.getType(geomref) == GeometryUtils.OGCType.ST_POLYGON) {
			try {
				OGCLineString hole = ((OGCPolygon)(ogcGeometry)).interiorRingN(idx);
				return GeometryUtils.geometryToEsriShapeBytesWritable(hole);
			} catch (Exception e) {
				LogUtils.Log_InternalError(LOG, "ST_InteriorRingN: " + e);
				return null;
			}
		} else {
			LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POLYGON, GeometryUtils.getType(geomref));
			return null;
		}
	}

}
