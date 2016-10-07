package com.esri.hadoop.hive;

import static org.junit.Assert.*;
import org.junit.Test;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class TestStY {

	@Test
	public void TestStY() {
		ST_Y stY = new ST_Y();
		ST_Point stPt = new ST_Point();
		BytesWritable bwGeom = stPt.evaluate(new DoubleWritable(1.2),
											 new DoubleWritable(3.4));
		DoubleWritable dwy = stY.evaluate(bwGeom);
		assertEquals(3.4, dwy.get(), .000001);
		bwGeom = stPt.evaluate(new DoubleWritable(6.5),
							   new DoubleWritable(4.3),
							   new DoubleWritable(2.1));
		dwy = stY.evaluate(bwGeom);
		assertEquals(4.3, dwy.get(), 0.0);
	}

}

