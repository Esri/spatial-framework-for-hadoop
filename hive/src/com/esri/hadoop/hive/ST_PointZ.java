package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
		name = "ST_PointZ",
		value = "_FUNC_(x, y, z) - constructor for 3D point",
		extended = "Example:\n" + 
		"SELECT _FUNC_(longitude, latitude, elevation) from src LIMIT 1;")
public class ST_PointZ extends ST_Geometry {
	
	public BytesWritable evaluate(DoubleWritable x, DoubleWritable y, DoubleWritable z){
		return evaluate(x, y, z, null);
	}

	// ZM
	public BytesWritable evaluate(DoubleWritable x, DoubleWritable y, DoubleWritable z, DoubleWritable m) {
		if (x == null || y == null || z == null) {
			return null;
		}
		Point stPt = new Point(x.get(), y.get(), z.get());
		if (m != null)
			stPt.setM(m.get());
		return GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(stPt, null));
	}
}
