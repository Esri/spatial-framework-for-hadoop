package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
// DoubleWritable - must use hive-serde2; the other one produces struct {value:d.d}
import org.apache.hadoop.hive.serde2.io.DoubleWritable;


import com.esri.core.geometry.ogc.OGCGeometry;

@Description(name = "ST_Area",
   value = "_FUNC_(ST_Polygon) - returns the area of polygon or multipolygon",
   extended = "Example:\n"
   + "  SELECT _FUNC_(ST_Polygon(1,1, 1,4, 4,4, 4,1)) FROM src LIMIT 1;  --  9.0"
)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_Area(ST_Polygon(1,1, 1,4, 4,4, 4,1)) from onerow",
//			result = "9.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Area(ST_Polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))')) from onerow",
//			result = "24.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Area(ST_MultiPolygon(array(1,1, 1,2, 2,2, 2,1), array(3,3, 3,4, 4,4, 4,3))) from onerow",
//			result = "2.0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Area(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_Area extends ST_GeometryAccessor {
	final DoubleWritable resultDouble = new DoubleWritable();
	static final Log LOG = LogFactory.getLog(ST_Area.class.getName());

	public DoubleWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		resultDouble.set(ogcGeometry.getEsriGeometry().calculateArea2D());
		return resultDouble;
	}
}
