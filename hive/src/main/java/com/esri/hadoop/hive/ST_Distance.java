package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
// DoubleWritable - must use hive-serde2; the other one produces struct {value:d.d}
import org.apache.hadoop.hive.serde2.io.DoubleWritable;


import com.esri.core.geometry.ogc.OGCGeometry;

@Description(name = "ST_Distance",
   value = "_FUNC_(ST_Geometry1, ST_Geometry2) - returns the distance between 2 ST_Geometry objects",
   extended = "Example:\n"
   + "  SELECT _FUNC_(ST_Point(0.0,0.0), ST_Point(3.0,4.0)) FROM src LIMIT 1;  --  5.0"
)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_Distance(ST_Point(0.0,0.0), ST_Point(3.0,4.0)) from onerow",
//			result = "5.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Distance(ST_LineString(0,0, 1,1), ST_LineString(2,1, 3,0)) from onerow",
//			result = "11"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Distance(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_Distance extends ST_GeometryAccessor {
	final DoubleWritable resultDouble = new DoubleWritable();
	static final Log LOG = LogFactory.getLog(ST_Distance.class.getName());

    public DoubleWritable evaluate(BytesWritable geometryref1, BytesWritable geometryref2) {
		if (geometryref1 == null || geometryref2 == null ||
		    geometryref1.getLength() == 0 || geometryref2.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}
		if (!GeometryUtils.compareSpatialReferences(geometryref1, geometryref2)) {
			LogUtils.Log_SRIDMismatch(LOG, geometryref1, geometryref2);
			return null;
		}

		OGCGeometry ogcGeom1 = GeometryUtils.geometryFromEsriShape(geometryref1);
		OGCGeometry ogcGeom2 = GeometryUtils.geometryFromEsriShape(geometryref2);
		if (ogcGeom1 == null || ogcGeom2 == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		try {
		    resultDouble.set(ogcGeom1.distance(ogcGeom2));
		    return resultDouble;
		} catch (Exception e) {
		    LogUtils.Log_InternalError(LOG, "ST_Distance: " + e);
		    return null;
		}

	}
}
