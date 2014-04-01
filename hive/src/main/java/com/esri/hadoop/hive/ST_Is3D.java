package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;


import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_Is3D",
	value = "_FUNC_(geometry) - return true if the geometry object is three-dimensional",
	extended = "Example:\n"
	+ "  > SELECT _FUNC_(ST_Polygon(1,1, 1,4, 4,4, 4,1)) FROM src LIMIT 1;  -- false\n"
	+ "  > SELECT _FUNC_(ST_LineString(0.,0., 3.,4., 0.,4., 0.,0.)) FROM src LIMIT 1;  -- false\n"
	+ "  > SELECT _FUNC_(ST_Point(3., 4.)) FROM src LIMIT 1;  -- false\n"
	+ "  > SELECT _FUNC_(ST_PointZ(3., 4., 2)) FROM src LIMIT 1;  -- true\n"
	)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_Is3D(ST_Point(0., 3.)) from onerow",
//			result = "false"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Is3D(ST_PointZ(0., 3., 1)) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Is3D(ST_Point('pointzm (0. 3. 1. 2.)')) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Is3D(null) from onerow",
//			result = "null"
//			)
//	}
//)

public class ST_Is3D extends ST_GeometryAccessor {
	final BooleanWritable resultBoolean = new BooleanWritable();
	static final Log LOG = LogFactory.getLog(ST_Is3D.class.getName());

	public BooleanWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		resultBoolean.set(ogcGeometry.is3D());
		return resultBoolean;
	}

}
