package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;


import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_IsEmpty",
	value = "_FUNC_(geometry) - return true if the geometry object is empty of geometric information",
	extended = "Example:\n"
	+ "  > SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  -- false\n"
	+ "  > SELECT _FUNC_(ST_GeomFromText('point empty')) FROM src LIMIT 1;  -- true\n"
	)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_IsEmpty(ST_GeomFromText('point empty')) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsEmpty(ST_Intersection(st_point(2,0), ST_Point(1,1))) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsEmpty(ST_GeomFromText('point (10.02 20.01)')) from onerow",
//			result = "false"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsEmpty(null) from onerow",
//			result = "null"
//			)
//		}
//	)

public class ST_IsEmpty extends ST_GeometryAccessor {
	final BooleanWritable resultBoolean = new BooleanWritable();
	static final Log LOG = LogFactory.getLog(ST_IsEmpty.class.getName());

	public BooleanWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		try {
			resultBoolean.set(ogcGeometry.isEmpty());
		} catch (Exception e) {
		    LogUtils.Log_InternalError(LOG, "ST_IsEmpty" + e);
			return null;
		}
		return resultBoolean;
	}
}
