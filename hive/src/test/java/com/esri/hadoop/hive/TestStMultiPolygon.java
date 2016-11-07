package com.esri.hadoop.hive;

import static org.junit.Assert.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

// select ST_AsText(ST_MultiPolygon(array(0.0,1.0, 1.0,1.0, 1.0,0.0)));
// select ST_Area(ST_MultiPolygon(array(0.0,0.0, 1.0,0.0, 1.0,1.0, 0.0,1.0)));


public class TestStMultiPolygon {

	DoubleWritable dw0 = new DoubleWritable(0);
	DoubleWritable dw1 = new DoubleWritable(1);
	ST_Area stArea = new ST_Area();
	ST_Equals stEquals = new ST_Equals();
	ST_ExteriorRing stExteriorRing = new ST_ExteriorRing();
	// ST_GeometryN stGeomN = new ST_GeometryN();
	ST_GeometryType typer = new ST_GeometryType();
	ST_SetSRID stSrid = new ST_SetSRID();
	String expty = "ST_MULTIPOLYGON";

	@Test
	public void TestBasic() throws Exception {
		ST_MultiPolygon stMultiPolygon = new ST_MultiPolygon();
		//DoubleWritable[] args = {dw0,dw0, dw0,dw1, dw1,dw0};
		List<DoubleWritable> args = new ArrayList<DoubleWritable>(7);
		args.add(dw0); args.add(dw0);
		args.add(dw0); args.add(dw1);
		args.add(dw1); args.add(dw0);
		BytesWritable rslt = stMultiPolygon.evaluate(args);
		Text gty = typer.evaluate(rslt);
		assertEquals(expty, gty.toString());
	}

	@Test
	public void TestStart() throws Exception {  // #110
		ST_MultiPolygon stMultiPolygon = new ST_MultiPolygon();
		List<DoubleWritable> args = new ArrayList<DoubleWritable>(7);
		args.add(dw0); args.add(dw1);
		args.add(dw1); args.add(dw0);
		args.add(dw0); args.add(dw0);
		BytesWritable rslt = stMultiPolygon.evaluate(args);
		// Text gty = typer.evaluate(rslt);
		// assertEquals(expty, gty.toString());
		// BytesWritable bwpg = stGeomN.evaluate(rslt, 0);
		BytesWritable cmp = stMultiPolygon.evaluate(new Text("multipolygon(((0 1, 1 0, 0 0)))"));
		//DeferredObject[] args = {rslt, cmp};
		//assertTrue(stEquals.evaluate(args));
		assertEquals(0.5, stArea.evaluate(rslt).get(), 0);
	}

	@Test
	public void TestWinding() throws Exception {  // #110
		ST_MultiPolygon stMultiPolygon = new ST_MultiPolygon();
		List<DoubleWritable> args = new ArrayList<DoubleWritable>(7);
		args.add(dw0); args.add(dw0);
		args.add(dw1); args.add(dw0);
		args.add(dw0); args.add(dw1);
		BytesWritable rslt = stMultiPolygon.evaluate(args);
		Text gty = typer.evaluate(rslt);
		assertEquals(expty, gty.toString());
		//assertTrue(stArea.evaluate(rslt) > 0);
		assertEquals(0.5, stArea.evaluate(rslt).get(), 0);
	}

	@Test
	public void TestSrid() throws Exception {  // #109
		ST_MultiPolygon stMultiPolygon = new ST_MultiPolygon();
		List<DoubleWritable> args = new ArrayList<DoubleWritable>(7);
		args.add(dw0); args.add(dw0);
		args.add(dw1); args.add(dw0);
		args.add(dw0); args.add(dw1);
		BytesWritable made = stMultiPolygon.evaluate(args);
		Text gty = typer.evaluate(made);
		assertEquals(expty, gty.toString());
		BytesWritable rslt = stSrid.evaluate(made, new IntWritable(4326));
		gty = typer.evaluate(rslt);
		assertEquals(expty, gty.toString());
	}

}
