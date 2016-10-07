package com.esri.hadoop.hive;

import static org.junit.Assert.*;
import org.junit.Test;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

// select ST_GeometryType(ST_Point(0, 0)) from onerow;
// select ST_GeometryType(ST_Point('point (10.02 20.01)')) from onerow;
// select ST_GeometryType(ST_Point('point z (10.02 20.01 2)')) from onerow;

public class TestStPoint {

	@Test
	public void TestStPoint() throws Exception {
		ST_GeometryType typer = new ST_GeometryType();
		ST_X stX = new ST_X();
		ST_Y stY = new ST_Y();
		ST_Point stPt = new ST_Point();
		BytesWritable bwGeom = stPt.evaluate(new DoubleWritable(1.2),
											 new DoubleWritable(3.4));
		DoubleWritable dwx = stX.evaluate(bwGeom);
		DoubleWritable dwy = stY.evaluate(bwGeom);
		assertEquals(1.2, dwx.get(), .000001);
		assertEquals(3.4, dwy.get(), .000001);
		Text gty = typer.evaluate(bwGeom);
		assertEquals("ST_POINT", gty.toString());
		bwGeom = stPt.evaluate(new DoubleWritable(6.5),
							   new DoubleWritable(4.3),
							   new DoubleWritable(2.1));
		gty = typer.evaluate(bwGeom);
		assertEquals("ST_POINT", gty.toString());
		bwGeom = stPt.evaluate(new Text("point (10.02 20.01)"));
		gty = typer.evaluate(bwGeom);
		assertEquals("ST_POINT", gty.toString());
		bwGeom = stPt.evaluate(new Text("point z (10.02 20.01 2)"));
		gty = typer.evaluate(bwGeom);
		assertEquals("ST_POINT", gty.toString());
	}

}

