package com.esri.hadoop.hive;

import static org.junit.Assert.*;
import org.junit.Test;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

// select ST_MinY(ST_GeomFromGeoJson('{"type":"LineString", "coordinates":[[1,2], [3,4]]}')) from onerow;
// select ST_MinY(ST_Point(1,2)) from onerow;
// select ST_MinY(ST_LineString(1.5,2.5, 3.0,2.2)) from onerow;
// select ST_MinY(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)) from onerow;
// select ST_MinY(ST_MultiPoint(0,0, 2,2)) from onerow;
// select ST_MinY(ST_MultiLineString(array(1, 1, 2, 2), array(10, 10, 20, 20))) from onerow;
// select ST_MinY(ST_MultiPolygon(array(1,1, 1,2, 2,2, 2,1), array(3,3, 3,4, 4,4, 4,3))) from onerow;

public class TestStMinY {

	@Test
	public void TestStMinY() {
		ST_MinY stMinY = new ST_MinY();
		ST_Point stPt = new ST_Point();
	}

}
