package com.esri.hadoop.hive;

import static org.junit.Assert.*;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Geometry.Type;

public class TestStAsShape {

	private final static double Epsilon = 0.0001;
	
	@Test
	public void testPointAsShape() {
		ST_Point point = new ST_Point();
		final double longitude = 12.224;
		final double latitude = 51.829;
		BytesWritable pointAsWritable = point.evaluate(new DoubleWritable(longitude), new DoubleWritable(latitude));
		assertNotNull("The point writable must not be null!", pointAsWritable);
		
		ST_AsShape asShape = new ST_AsShape();
		BytesWritable shapeAsWritable = asShape.evaluate(pointAsWritable);
		assertNotNull("The shape writable must not be null!", pointAsWritable);
		
		byte[] esriShapeBuffer = shapeAsWritable.getBytes();
		Geometry esriGeometry = GeometryEngine.geometryFromEsriShape(esriShapeBuffer, Type.Point);
		assertNotNull("The geometry must not be null!", esriGeometry);
		assertTrue("Geometry type point expected!", esriGeometry instanceof Point);
		
		Point esriPoint = (Point) esriGeometry;
		assertEquals("Longitude is different!", longitude, esriPoint.getX(), Epsilon);
		assertEquals("Latitude is different!", latitude, esriPoint.getY(), Epsilon);
	}
}
