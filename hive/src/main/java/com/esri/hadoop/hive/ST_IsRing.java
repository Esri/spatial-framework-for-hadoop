package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;


import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;

@Description(
	name = "ST_IsRing",
	value = "_FUNC_(ST_LineString) - return true if the linestring is closed & simple",
	extended = "Example:\n"
	+ "  SELECT _FUNC_(ST_LineString(0.,0., 3.,4., 0.,4., 0.,0.)) FROM src LIMIT 1;  -- true\n"
	+ "  SELECT _FUNC_(ST_LineString(0.,0., 1.,1., 1.,2., 2.,1., 1.,1., 0.,0.)) FROM src LIMIT 1;  -- false\n"
	+ "  SELECT _FUNC_(ST_LineString(0.,0., 3.,4.)) FROM src LIMIT 1;  -- false\n"
	)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_IsRing(ST_LineString(0.,0., 3.,4., 0.,4., 0.,0.)) from onerow",
//			result = "true"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsRing(ST_LineString(0.,0., 3.,4.)) from onerow",
//			result = "false"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsRing(ST_LineString(0.,0., 1.,1., 1.,2., 2.,1., 1.,1., 0.,0.)) from onerow",
//			result = "false"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_IsRing(null) from onerow",
//			result = "null"
//			)
//		}
//	)

public class ST_IsRing extends ST_GeometryAccessor {
	final BooleanWritable resultBoolean = new BooleanWritable();
	static final Log LOG = LogFactory.getLog(ST_IsRing.class.getName());

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

			switch(GeometryUtils.getType(geomref)) {
			case ST_LINESTRING:
				OGCLineString lns = (OGCLineString)ogcGeometry;
				resultBoolean.set(lns.isClosed() && lns.isSimple());
				return resultBoolean;
			default:  // ST_IsRing gives ERROR on Point, Polygon, or MultiLineString - on Postgres
				LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_LINESTRING, GeometryUtils.getType(geomref));
				return null;
			}

		} catch (Exception e) {
		    LogUtils.Log_InternalError(LOG, "ST_IsRing" + e);
			return null;
		}
	}

}
