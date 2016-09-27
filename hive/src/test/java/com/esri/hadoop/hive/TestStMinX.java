package com.esri.hadoop.hive;

import static org.junit.Assert.*;
import org.junit.Test;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

// select ST_MinX(ST_Point(1,2)) from onerow;
// select ST_MinX(ST_LineString(1.5,2.5, 3.0,2.2)) from onerow;
// select ST_MinX(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)) from onerow;
// select ST_MinX(ST_MultiPoint(0,0, 2,2)) from onerow;
// select ST_MinX(ST_MultiLineString(array(1, 1, 2, 2), array(10, 10, 20, 20))) from onerow;
// select ST_MinX(ST_MultiPolygon(array(1,1, 1,2, 2,2, 2,1), array(3,3, 3,4, 4,4, 4,3))) from onerow;

public class TestStMinX {

	@Test
	public void TestStMinX() {
		ST_MinX stMinX = new ST_MinX();
		ST_Point stPt = new ST_Point();
		BytesWritable bwGeom = stPt.evaluate(new DoubleWritable(1.2),
											 new DoubleWritable(3.4));
		DoubleWritable dwx = stMinX.evaluate(bwGeom);
		assertEquals(1.2, dwx.get(), .000001);
		bwGeom = stPt.evaluate(new DoubleWritable(6.5),
							   new DoubleWritable(4.3),
							   new DoubleWritable(2.1));
		dwx = stMinX.evaluate(bwGeom);
		assertEquals(6.5, dwx.get(), 0.0);
	}

}
