package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Polygon;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_NumPoints",
	value = "_FUNC_(geometry) - return the number of points in the geometry",
	extended = "Example:\n"
	+ "  > SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  -- 1\n"
	+ "  > SELECT _FUNC_(ST_LineString(1.5,2.5, 3.0,2.2)) FROM src LIMIT 1;  -- 2\n"
	+ "  > SELECT _FUNC_(ST_GeomFromText('polygon ((0 0, 10 0, 0 10, 0 0))')) FROM src LIMIT 1;  -- 4\n"
	)
@HivePdkUnitTests(
	cases = {
		@HivePdkUnitTest(
			query = "select ST_NumPoints(ST_Point(0., 3.)) from onerow",
			result = "1"
			),
		@HivePdkUnitTest(
			query = "select ST_NumPoints(ST_LineString(0.,0., 3.,4.)) from onerow",
			result = "2"
			),
		@HivePdkUnitTest(
			query = "select ST_NumPoints(ST_GeomFromText('polygon ((0 0, 10 0, 0 10, 0 0))')) from onerow",
			result = "4"
			),
		@HivePdkUnitTest(
			query = "select ST_NumPoints(ST_GeomFromText('multipoint ((10 40), (40 30), (20 20), (30 10))', 0)) from onerow",
			result = "4"
			),
		@HivePdkUnitTest(
			query = "select ST_NumPoints(ST_GeomFromText('multilinestring ((2 4, 10 10), (20 20, 7 8))')) from onerow",
			result = "4"
			),
		@HivePdkUnitTest(
			query = "select ST_NumPoints(ST_Point('point empty')) from onerow",
			result = "0"
			)
		}
	)

public class ST_NumPoints extends ST_GeometryAccessor {
	public static final IntWritable resultInt = new IntWritable();
	static final Log LOG = LogFactory.getLog(ST_IsClosed.class.getName());

	public IntWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		Geometry esriGeom = ogcGeometry.getEsriGeometry();
		switch(esriGeom.getType()) {
		case Point:
			resultInt.set(esriGeom.isEmpty() ? 0 : 1);
			break;
		case MultiPoint:
			resultInt.set(((MultiPoint)(esriGeom)).getPointCount());
			break;
		case Polygon:
			Polygon polygon = (Polygon)(esriGeom);
		    resultInt.set(polygon.getPointCount() + polygon.getPathCount());
			break;
		default:
			resultInt.set(((MultiPath)(esriGeom)).getPointCount());
			break;
		}
		return resultInt;
	}
}
