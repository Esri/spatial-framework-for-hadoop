package com.esri.hadoop.hive;

import static org.junit.Assert.*;
import org.junit.Test;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class TestStX {

	@Test
	public void TestStX() {
		ST_X stX = new ST_X();
		ST_Point stPt = new ST_Point();
		BytesWritable bwGeom = stPt.evaluate(new DoubleWritable(1.2),
											 new DoubleWritable(3.4));
		DoubleWritable dwx = stX.evaluate(bwGeom);
		assertEquals(1.2, dwx.get(), .000001);
		bwGeom = stPt.evaluate(new DoubleWritable(6.5),
							   new DoubleWritable(4.3),
							   new DoubleWritable(2.1));
		dwx = stX.evaluate(bwGeom);
		assertEquals(6.5, dwx.get(), 0.0);
	}

}

