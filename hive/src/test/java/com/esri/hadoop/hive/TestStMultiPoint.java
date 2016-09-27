package com.esri.hadoop.hive;

import static org.junit.Assert.*;
import org.junit.Test;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

// select ST_GeometryType(ST_MultiPoint('multipoint ((1 2))')) from onerow;

public class TestStMultiPoint {

	@Test
	public void test() throws Exception {
		ST_GeometryType typer = new ST_GeometryType();
		ST_MultiPoint stMp = new ST_MultiPoint();
		BytesWritable bwGeom = stMp.evaluate(new Text("multipoint ((1 2))"));
		Text gty = typer.evaluate(bwGeom);
		assertEquals("ST_MULTIPOINT", gty.toString());
	}

}

