package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;


import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_GeomFromText",
	value = "_FUNC_(wkt) - construct an ST_Geometry from OGC well-known text",
	extended = "Example:\n"
	+ "  SELECT _FUNC_('linestring (1 0, 2 3)') FROM src LIMIT 1;  -- constructs ST_Linestring\n"
	+ "  SELECT _FUNC_('multipoint ((1 0), (2 3))') FROM src LIMIT 1;  -- constructs ST_MultiPoint\n"
	)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_AsText(ST_GeomFromText('point (10.02 20.01)')) from onerow",
//			result = "POINT (10.02 20.01)"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_AsText(ST_GeomFromText('linestring (10 10, 20 20)')) from onerow",
//			result = "LINESTRING (10 10, 20 20)"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_AsText(ST_GeomFromText('polygon ((0 0, 0 10, 10 10, 0 0))')) from onerow",
//			result = "POLYGON ((0 0, 0 10, 10 10, 0 0))"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_AsText(ST_GeomFromText('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))')) from onerow",
//			result = "MULTIPOINT (10 40, 40 30, 20 20, 30 10)"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_AsText(ST_GeomFromText('multilinestring ((2 4, 10 10), (20 20, 7 8))')) from onerow",
//			result = "MULTILINESTRING ((2 4, 10 10), (20 20, 7 8))"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_AsText(ST_GeomFromText('multipolygon (((0 0, 0 1, 1 0, 0 0)), ((2 2, 2 3, 3 2, 2 2)))')) from onerow",
//			result = "MULTIPOLYGON (((0 0, 0 1, 1 0, 0 0)), ((2 2, 2 3, 3 2, 2 2)))"
//			)
//		}
//	)

public class ST_GeomFromText extends ST_Geometry {

	static final Log LOG = LogFactory.getLog(ST_GeomFromText.class.getName());

	public BytesWritable evaluate(Text wkt) throws UDFArgumentException {
		return evaluate(wkt, 0);
	}

	public BytesWritable evaluate(Text wkwrap, int wkid) throws UDFArgumentException {

		String wkt = wkwrap.toString();
		try {
			SpatialReference spatialReference = null;
			if (wkid != GeometryUtils.WKID_UNKNOWN) {
				spatialReference = SpatialReference.create(wkid);
			}
			OGCGeometry ogcObj = OGCGeometry.fromText(wkt);
			ogcObj.setSpatialReference(spatialReference);
			return GeometryUtils.geometryToEsriShapeBytesWritable(ogcObj);
		} catch (Exception e) {  // IllegalArgumentException, GeometryException
			LogUtils.Log_InvalidText(LOG, wkt);
			return null;
		}
	}

}
