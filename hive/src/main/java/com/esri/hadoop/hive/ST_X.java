package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
// DoubleWritable - must use hive-serde2; the other one produces struct {value:d.d}
import org.apache.hadoop.hive.serde2.io.DoubleWritable;


import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;

@Description(name = "ST_X",
   value = "_FUNC_(point) - returns the X coordinate of point",
   extended = "Example:\n"
   + "  SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  --  1.5"
)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_X(ST_Point(1,2)) from onerow",
//			result = "1"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_X(ST_LineString(1.5,2.5, 3.0,2.2)) from onerow",
//			result = "null"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_X(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_X extends ST_GeometryAccessor {
	final DoubleWritable resultDouble = new DoubleWritable();
	static final Log LOG = LogFactory.getLog(ST_X.class.getName());

	public DoubleWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			return null;
		}

		switch(GeometryUtils.getType(geomref)) {
		case ST_POINT:
			OGCPoint pt = (OGCPoint)ogcGeometry;
			resultDouble.set(pt.X());
			return resultDouble;
		default:
			LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POINT, GeometryUtils.getType(geomref));
			return null;
		}
	}

}
