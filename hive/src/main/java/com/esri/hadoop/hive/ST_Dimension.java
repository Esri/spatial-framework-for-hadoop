package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;


import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_Dimension",
	value = "_FUNC_(geometry) - return spatial dimension of geometry",
	extended = "Example:\n"
	+ "  > SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  -- 0\n"
	+ "  > SELECT _FUNC_(ST_LineString(1.5,2.5, 3.0,2.2)) FROM src LIMIT 1;  -- 1\n"
	+ "  > SELECT _FUNC_(ST_Polygon(2,0, 2,3, 3,0)) FROM src LIMIT 1;  -- 2\n"
	)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_Dimension(ST_Point(0,0)) from onerow",
//			result = "0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Dimension(ST_LineString(1.5,2.5, 3.0,2.2)) from onerow",
//			result = "1"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Dimension(ST_Polygon(1.5,2.5, 3.0,2.2, 2.2,1.1)) from onerow",
//			result = "2"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Dimension(ST_MultiPoint(0,0, 2,2)) from onerow",
//			result = "0"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Dimension(ST_MultiLineString(array(1, 1, 2, 2), array(10, 10, 20, 20))) from onerow",
//			result = "1"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Dimension(ST_MultiPolygon(array(1,1, 1,2, 2,2, 2,1), array(3,3, 3,4, 4,4, 4,3))) from onerow",
//			result = "2"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Dimension(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_Dimension extends ST_GeometryAccessor {
	final IntWritable resultInt = new IntWritable();
	static final Log LOG = LogFactory.getLog(ST_Dimension.class.getName());

	public IntWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		resultInt.set(ogcGeometry.dimension());
		return resultInt;
	}

}
