package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;


import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPolygon;

@Description(
	name = "ST_NumInteriorRing",
	value = "_FUNC_(ST_Polygon) - return the number of interior rings in the polygon",
	extended = "Example:\n"
	+ "  SELECT _FUNC_(ST_Polygon(1,1, 1,4, 4,1)) FROM src LIMIT 1;  -- 0\n"
	+ "  SELECT _FUNC_(ST_Polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))')) FROM src LIMIT 1;  -- 1\n"
	)

//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_NumInteriorRing(ST_Polygon('polygon ((1 1, 4 1, 1 4))')) from onerow",
//			result = "0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_NumInteriorRing(ST_Polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))')) from onerow",
//			result = "1"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_NumInteriorRing(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_NumInteriorRing extends ST_GeometryAccessor {
	static final Log LOG = LogFactory.getLog(ST_NumInteriorRing.class.getName());
	final IntWritable resultInt = new IntWritable();

	public IntWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}
		if (GeometryUtils.getType(geomref) == GeometryUtils.OGCType.ST_POLYGON) {
			try {
				resultInt.set(((OGCPolygon)(ogcGeometry)).numInteriorRing());
				return resultInt;
			} catch (Exception e) {
				LogUtils.Log_InternalError(LOG, "ST_NumInteriorRing: " + e);
				return null;
			}
		} else {
			LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POLYGON, GeometryUtils.getType(geomref));
			return null;
		}
	}

}
