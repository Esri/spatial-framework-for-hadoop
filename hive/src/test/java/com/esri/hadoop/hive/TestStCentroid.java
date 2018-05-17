package com.esri.hadoop.hive;

import static org.junit.Assert.*;
import org.junit.Test;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import com.esri.core.geometry.Point;


public class TestStCentroid {

	private final static double Epsilon = 0.0001;

	@Test
	public void TestSimplePointCentroid() throws Exception {
		final ST_Centroid stCtr = new ST_Centroid();
		final ST_Point stPt = new ST_Point();
		BytesWritable bwGeom = stPt.evaluate(new Text("point (2 3)"));
		BytesWritable bwCentroid = stCtr.evaluate(bwGeom);
		validatePoint(new Point(2,3), bwCentroid);
	}

	@Test
	public void TestMultiPointCentroid() throws Exception {
		final ST_Centroid stCtr = new ST_Centroid();
		final ST_MultiPoint stMp = new ST_MultiPoint();
		BytesWritable bwGeom = stMp.evaluate(new Text("multipoint ((0 0), (1 1), (1 -1), (6 0))"));
		BytesWritable bwCentroid = stCtr.evaluate(bwGeom);
		validatePoint(new Point(2,0), bwCentroid);
	}

	@Test
	public void TestLineCentroid() throws Exception {
		final ST_Centroid stCtr = new ST_Centroid();
		final ST_LineString stLn = new ST_LineString();
		BytesWritable bwGeom = stLn.evaluate(new Text("linestring (0 0, 6 0)"));
		BytesWritable bwCentroid = stCtr.evaluate(bwGeom);
		validatePoint(new Point(3,0), bwCentroid);
		bwGeom = stLn.evaluate(new Text("linestring (0 0, 2 4, 6 8)"));
		bwCentroid = stCtr.evaluate(bwGeom);
		validatePoint(new Point(3,4), bwCentroid);
	}

	@Test
	public void TestPolygonCentroid() throws Exception {
		final ST_Centroid stCtr = new ST_Centroid();
		final ST_Polygon stPoly = new ST_Polygon();
		BytesWritable bwGeom = stPoly.evaluate(new Text("polygon ((0 0, 0 8, 8 8, 8 0, 0 0))"));
		BytesWritable bwCentroid = stCtr.evaluate(bwGeom);
		validatePoint(new Point(4,4), bwCentroid);
		bwGeom = stPoly.evaluate(new Text("polygon ((1 1, 5 1, 3 4))"));
		bwCentroid = stCtr.evaluate(bwGeom);
		validatePoint(new Point(3,2), bwCentroid);
	}

	/**
	 * Validates the geometry writable.
	 * 
	 * @param point
	 *            the represented point location.
	 * @param geometryAsWritable
	 *            the geometry represented as {@link BytesWritable}.
	 */
	private static void validatePoint(Point point, BytesWritable geometryAsWritable) {
		ST_X getX = new ST_X();
		DoubleWritable xAsWritable = getX.evaluate(geometryAsWritable);
		assertNotNull("The x writable must not be null!", xAsWritable);

		ST_Y getY = new ST_Y();
		DoubleWritable yAsWritable = getY.evaluate(geometryAsWritable);
		assertNotNull("The y writable must not be null!", yAsWritable);

		assertEquals("Longitude is different!", point.getX(), xAsWritable.get(), Epsilon);
		assertEquals("Latitude is different!", point.getY(), yAsWritable.get(), Epsilon);
	}

}
