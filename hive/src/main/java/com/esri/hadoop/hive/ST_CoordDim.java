package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;


import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_CoordDim",
	value = "_FUNC_(geometry) - return count of coordinate components",
	extended = "Example:\n"
	+ "  > SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  -- 2\n"
	+ "  > SELECT _FUNC_(ST_PointZ(1.5,2.5, 3) FROM src LIMIT 1;  -- 3\n"
	+ "  > SELECT _FUNC_(ST_Point(1.5, 2.5, 3., 4.)) FROM src LIMIT 1;  -- 4\n"
	)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_CoordDim(ST_Point(0., 3.)) from onerow",
//			result = "2"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_CoordDim(ST_PointZ(0., 3., 1)) from onerow",
//			result = "3"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_CoordDim(ST_Point(0., 3., 1., 2.)) from onerow",
//			result = "4"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_CoordDim(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_CoordDim extends ST_GeometryAccessor {
	final IntWritable resultInt = new IntWritable();
	static final Log LOG = LogFactory.getLog(ST_Is3D.class.getName());

	public IntWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			return null;
		}

		resultInt.set(ogcGeometry.coordinateDimension());
		return resultInt;
	}

}
