package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;


@Description(
	name = "ST_GeometryType",
	value = "_FUNC_(geometry) - return type of geometry",
	extended = "Example:\n"
	+ "  > SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1;  -- ST_Point\n"
	+ "  > SELECT _FUNC_(ST_LineString(1.5,2.5, 3.0,2.2)) FROM src LIMIT 1;  -- ST_LineString\n"
	+ "  > SELECT _FUNC_(ST_Polygon(2,0, 2,3, 3,0)) FROM src LIMIT 1;  -- ST_Polygon\n"
	)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_GeomFromText('point (10.02 20.01)')) from onerow",
//			result = "ST_POINT"
//			),
//		@HivePdkUnitTest(
//			query = "selectST_GeometryType(ST_GeomFromText('linestring (10 10, 20 20)')) from onerow",
//			result = "ST_LINESTRING"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_GeomFromText('polygon ((0 0, 0 10, 10 10, 0 0))')) from onerow",
//			result = "ST_POLYGON"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_GeomFromText('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))')) from onerow",
//			result = "ST_MULTIPOINT"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_GeomFromText('multilinestring ((2 4, 10 10), (20 20, 7 8))')) from onerow",
//			result = "ST_MULTILINESTRING"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_GeomFromText('multipolygon (((0 0, 0 1, 1 0, 0 0)), ((2 2, 2 3, 3 2, 2 2)))')) from onerow",
//			result = "ST_MULTIPOLYGON"
//			)
//		}
//	)

public class ST_GeometryType extends ST_Geometry {
	static final Log LOG = LogFactory.getLog(ST_GeometryType.class.getName());

	public Text evaluate(BytesWritable ref) {
		if (ref == null || ref.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}
		return new Text(GeometryUtils.getType(ref).toString());
	}
}
