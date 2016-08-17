package com.esri.hadoop.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Point;

public class TestStGeomFromShape {
	
	private final static double Epsilon = 0.0001;

	@Test
	public void testGeomFromPointShape() throws UDFArgumentException {
		final double longitude = 12.224;
		final double latitude = 51.829;
		Point point = new Point(longitude, latitude);
		byte[] esriShape = GeometryEngine.geometryToEsriShape(point);
		assertNotNull("The point writable must not be null!", esriShape);
		
		BytesWritable shapeAsWritable = new BytesWritable(esriShape);
		assertNotNull("The shape writable must not be null!", shapeAsWritable);
		
		ST_GeomFromShape fromShape = new ST_GeomFromShape();
		BytesWritable geometryAsWritable = fromShape.evaluate(shapeAsWritable);
		
		ST_X getX = new ST_X();
		DoubleWritable xAsWritable = getX.evaluate(geometryAsWritable);
		assertNotNull("The x writable must not be null!", xAsWritable);
		
		ST_Y getY = new ST_Y();
		DoubleWritable yAsWritable = getY.evaluate(geometryAsWritable);
		assertNotNull("The y writable must not be null!", yAsWritable);
		
		assertEquals("Longitude is different!", longitude, xAsWritable.get(), Epsilon);
		assertEquals("Latitude is different!", latitude, yAsWritable.get(), Epsilon);
		
		ST_SRID getWkid = new ST_SRID();
		IntWritable wkidAsWritable = getWkid.evaluate(geometryAsWritable);
		assertNotNull("The wkid writable must not be null!", wkidAsWritable);
		
		assertEquals("The wkid is different!", 0, wkidAsWritable.get());
	}
}
