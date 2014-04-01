package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
// DoubleWritable - must use hive-serde2; the other one produces struct {value:d.d}
import org.apache.hadoop.hive.serde2.io.DoubleWritable;


import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(name = "ST_MaxX",
   value = "_FUNC_(geometry) - returns the maximum X coordinate of geometry",
   extended = "Example:\n"
   + "  > SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  -- 1.5\n"
   + "  > SELECT _FUNC_(ST_LineString(1.5,2.5, 3.0,2.2)) FROM src LIMIT 1;  -- 3.0\n"
)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_MaxX(ST_Point(1,2)) from onerow",
//			result = "1"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxX(ST_LineString(1.5,2.5, 3.0,2.2)) from onerow",
//			result = "3.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxX(ST_Polygon(1,1, 1,4, 4,4, 4,1)) from onerow",
//			result = "4"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxX(ST_MultiPoint(0,0, 2,2)) from onerow",
//			result = "2"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxX(ST_MultiLineString(array(1, 1, 2, 2), array(10, 10, 20, 20))) from onerow",
//			result = "20"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxX(ST_MultiPolygon(array(1,1, 1,2, 2,2, 2,1), array(3,3, 3,4, 4,4, 4,3))) from onerow",
//			result = "4"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_MaxX(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_MaxX extends ST_GeometryAccessor {
	final DoubleWritable resultDouble = new DoubleWritable();
	static final Log LOG = LogFactory.getLog(ST_MaxX.class.getName());

	public DoubleWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		Envelope envBound = new Envelope();
		ogcGeometry.getEsriGeometry().queryEnvelope(envBound);
		resultDouble.set(envBound.getXMax());
		return resultDouble;
	}
}
