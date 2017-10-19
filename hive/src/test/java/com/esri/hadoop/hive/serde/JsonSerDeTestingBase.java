package com.esri.hadoop.hive.serde;

import org.junit.Assert;
import java.util.ArrayList;

import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;

public abstract class JsonSerDeTestingBase {

	protected void addWritable(ArrayList<Object> stuff, boolean item) {
		stuff.add(new BooleanWritable(item));
	}

	protected void addWritable(ArrayList<Object> stuff, byte item) {
		stuff.add(new ByteWritable(item));
	}

	protected void addWritable(ArrayList<Object> stuff, short item) {
		stuff.add(new ShortWritable(item));
	}

	protected void addWritable(ArrayList<Object> stuff, int item) {
		stuff.add(new IntWritable(item));
	}

	protected void addWritable(ArrayList<Object> stuff, long item) {
		stuff.add(new LongWritable(item));
	}

	protected void addWritable(ArrayList<Object> stuff, String item) {
		stuff.add(new Text(item));
	}

	protected void addWritable(ArrayList<Object> stuff, java.sql.Date item) {
		stuff.add(new DateWritable(item));
	}

	protected void addWritable(ArrayList<Object> stuff, java.sql.Timestamp item) {
		stuff.add(new TimestampWritable(item));
	}

	protected void addWritable(ArrayList<Object> stuff, Geometry geom) {
		addWritable(stuff, geom, null);
	}

	protected void addWritable(ArrayList<Object> stuff, MapGeometry geom) {
		addWritable(stuff, geom.getGeometry(), geom.getSpatialReference());
	}

	protected void addWritable(ArrayList<Object> stuff, Geometry geom, SpatialReference sref) {
		stuff.add(GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(geom, sref)));
	}

    protected void ckPoint(Point refPt, BytesWritable fieldData) {
		Assert.assertEquals(refPt,
							GeometryUtils.geometryFromEsriShape(fieldData).getEsriGeometry());
	}

	protected Object getField(String col, Object row, StructObjectInspector rowOI) {
		StructField f0 = rowOI.getStructFieldRef(col);
		return rowOI.getStructFieldData(row, f0);
	}

	protected Object runSerDe(Object stuff, AbstractSerDe jserde, StructObjectInspector rowOI) throws Exception {
		Writable jsw = jserde.serialize(stuff, rowOI);
		//System.err.println(jsw);
		return jserde.deserialize(jsw);
	}

}
