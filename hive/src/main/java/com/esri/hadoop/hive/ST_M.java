package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
// DoubleWritable - must use hive-serde2; the other one produces struct {value:d.d}
import org.apache.hadoop.hive.serde2.io.DoubleWritable;


import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;

@Description(
	name = "ST_M",
	value = "_FUNC_(geometry) - return true if the geometry object is three-dimensional",
	extended = "Example:\n"
	+ "  > SELECT _FUNC_(ST_PointM(3., 4., 2)) FROM src LIMIT 1;  -- 2\n"
	)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_M(ST_Point('point m(0. 3. 1)')) from onerow",
//			result = "1.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_M(ST_Point('pointzm (0. 3. 1. 2.)')) from onerow",
//			result = "2.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_M(ST_Point(0., 3.)) from onerow",
//			result = "null"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_M(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_M extends ST_GeometryAccessor {
	final DoubleWritable resultDouble = new DoubleWritable();
	static final Log LOG = LogFactory.getLog(ST_M.class.getName());

	public DoubleWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			return null;
		}
		if (!ogcGeometry.isMeasured()) {
			LogUtils.Log_NotMeasured(LOG);
			return null;
		}

		switch(GeometryUtils.getType(geomref)) {
		case ST_POINT:
			OGCPoint pt = (OGCPoint)ogcGeometry;
			resultDouble.set(pt.M());
			return resultDouble;
		default:
			LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POINT, GeometryUtils.getType(geomref));
			return null;
		}
	}

}
