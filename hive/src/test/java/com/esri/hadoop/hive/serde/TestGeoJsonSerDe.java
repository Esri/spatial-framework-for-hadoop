package com.esri.hadoop.hive.serde;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.*;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import com.esri.hadoop.shims.HiveShims;

// Ideally tests to cover:
//  - attributes and/or geometry
//  - null attributes and values to not linger
//  - null geometry
//  - spatial reference preserved

public class TestGeoJsonSerDe {

	@Test
	public void TestIntWrite() throws Exception {  // Is this valid for GeoJSON?
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
		SerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        // {"properties":{"num":7}}
        addWritable(stuff, 7);
		Writable jsw = jserde.serialize(stuff, rowOI);
		JsonNode jn = new ObjectMapper().readTree(((Text)jsw).toString());
		jn = jn.findValue("properties");
		jn = jn.findValue("num");
		Assert.assertEquals(7, jn.getIntValue());
	}

	@Test
	public void TestPointWrite() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
		SerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        // {"properties":{},"geometry":{"type":"Point","coordinates":[15.0,5.0]}}
        addWritable(stuff, new Point(15.0, 5.0));
		Writable jsw = jserde.serialize(stuff, rowOI);
        String rslt = ((Text)jsw).toString();
		JsonNode jn = new ObjectMapper().readTree(rslt);
		jn = jn.findValue("geometry");
		Assert.assertNotNull(jn.findValue("type"));
		Assert.assertNotNull(jn.findValue("coordinates"));
	}

	@Test
	public void TestIntParse() throws Exception {  // Is this valid for GeoJSON?
		Configuration config = new Configuration();
		Text value = new Text();

		SerDe jserde = new GeoJsonSerDe();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        value.set("{\"properties\":{\"num\":7}}");
		Object row = jserde.deserialize(value);
		StructField f0 = rowOI.getStructFieldRef("num");
		Object fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(7, ((IntWritable)fieldData).get());
        value.set("{\"properties\":{\"num\":9}}");
        row = jserde.deserialize(value);
		f0 = rowOI.getStructFieldRef("num");
		fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(9, ((IntWritable)fieldData).get());
	}

	@Test
	public void TestPointParse() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		SerDe jserde = new GeoJsonSerDe();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        value.set("{\"properties\":{},\"geometry\":{\"type\":\"Point\",\"coordinates\":[15.0,5.0]}}");
		Object row = jserde.deserialize(value);
		StructField f0 = rowOI.getStructFieldRef("shape");
		Object fieldData = rowOI.getStructFieldData(row, f0);
		ckPoint(new Point(15.0, 5.0), (BytesWritable)fieldData);

        value.set("{\"properties\":{},\"geometry\":{\"type\":\"Point\",\"type\":\"Point\",\"coordinates\":[7.0,4.0]}}");
        row = jserde.deserialize(value);
		f0 = rowOI.getStructFieldRef("shape");
		fieldData = rowOI.getStructFieldData(row, f0);
		ckPoint(new Point(7.0, 4.0), (BytesWritable)fieldData);
	}

	@Test
	public void TestIntOnly() throws Exception {  // Is this valid for GeoJSON?
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
		SerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"properties\":{\"num\":7}}");
        addWritable(stuff, 7);
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("num", row, rowOI);
		Assert.assertEquals(7, ((IntWritable)fieldData).get());
		stuff.clear();
		addWritable(stuff, 9);
		row = runSerDe(stuff, jserde, rowOI);
		fieldData = getField("num", row, rowOI);
		Assert.assertEquals(9, ((IntWritable)fieldData).get());
	}

	@Test
	public void TestPointOnly() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
		SerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"properties\":{},\"geometry\":{\"type\":\"Point\",\"coordinates\":[15.0,5.0]}}");
        addWritable(stuff, new Point(15.0, 5.0));
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("shape", row, rowOI);
		ckPoint(new Point(15.0, 5.0), (BytesWritable)fieldData);

        //value.set("{\"properties\":{},\"geometry\":{\"type\":\"Point\",\"type\":\"Point\",\"coordinates\":[7.0,4.0]}}");
		stuff.clear();
        addWritable(stuff, new Point(7.0, 4.0));
		row = runSerDe(stuff, jserde, rowOI);
		fieldData = getField("shape", row, rowOI);
		ckPoint(new Point(7.0, 4.0), (BytesWritable)fieldData);
	}

	@Test
	public void TestIntPoint() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num,shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "bigint,binary");
		SerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        // value.set("{\"properties\":{\"num\":7},\"geometry\":{\"type\":\"Point\",\"coordinates\":[15.0,5.0]}}");
        addWritable(stuff, 7L);
        addWritable(stuff, new Point(15.0, 5.0));
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("num", row, rowOI);
		Assert.assertEquals(7, ((LongWritable)fieldData).get());

        //value.set("{\"properties\":{\"num\":4},\"geometry\":{\"type\":\"Point\",\"coordinates\":[7.0,2.0]}}");
		stuff.clear();
        addWritable(stuff, 4L);
        addWritable(stuff, new Point(7.0, 2.0));
		row = runSerDe(stuff, jserde, rowOI);
		fieldData = getField("num", row, rowOI);
		Assert.assertEquals(4, ((LongWritable)fieldData).get());
		fieldData = getField("shape", row, rowOI);
		ckPoint(new Point(7.0, 2.0), (BytesWritable)fieldData);
	}

	@Test
	public void TestNullAttr() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
		SerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"properties\":{\"num\":7}}");
		addWritable(stuff, 7);
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("num", row, rowOI);
		Assert.assertEquals(7, ((IntWritable)fieldData).get());
        //value.set("{\"properties\":{}}");
		stuff.set(0, null);
		row = runSerDe(stuff, jserde, rowOI);
		fieldData = getField("num", row, rowOI);
		Assert.assertNull(fieldData);
	}

	@Test
	public void TestNullGeom() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
		SerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"properties\":{},\"geometry\":{\"type\":\"Point\",\"coordinates\":[15.0,5.0]}}");
        addWritable(stuff, new Point(15.0, 5.0));
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("shape", row, rowOI);
		ckPoint(new Point(15.0, 5.0), (BytesWritable)fieldData);

        //value.set("{\"properties\":{},\"coordinates\":null}");
		stuff.set(0, null);
		row = runSerDe(stuff, jserde, rowOI);
		fieldData = getField("shape", row, rowOI);
		Assert.assertNull(fieldData);
	}


	private void addWritable(ArrayList<Object> stuff, int item) {
		stuff.add(new IntWritable(item));
	}

	private void addWritable(ArrayList<Object> stuff, long item) {
		stuff.add(new LongWritable(item));
	}

	private void addWritable(ArrayList<Object> stuff, Geometry geom) {
		//stuff.add(GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(geom, null)));
		addWritable(stuff, geom, null);
	}

	private void addWritable(ArrayList<Object> stuff, MapGeometry geom) {
		//stuff.add(GeometryUtils.geometryToEsriShapeBytesWritable(
        //          OGCGeometry.createFromEsriGeometry(geom.getGeometry(), geom.getSpatialReference())));
		addWritable(stuff, geom.getGeometry(), geom.getSpatialReference());
	}

	private void addWritable(ArrayList<Object> stuff, Geometry geom, SpatialReference sref) {
		stuff.add(GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(geom, sref)));
	}

    private void ckPoint(Point refPt, BytesWritable fieldData) {
		Assert.assertEquals(refPt,
							GeometryUtils.geometryFromEsriShape(fieldData).getEsriGeometry());
	}

	private Object getField(String col, Object row, StructObjectInspector rowOI) {
		StructField f0 = rowOI.getStructFieldRef(col);
		return rowOI.getStructFieldData(row, f0);
	}

	private SerDe mkSerDe(Properties proptab) throws Exception {
		Configuration config = new Configuration();
		SerDe jserde = new GeoJsonSerDe();
		jserde.initialize(config, proptab);
		return jserde;
	}

	private Object runSerDe(Object stuff, SerDe jserde, StructObjectInspector rowOI) throws Exception {
		Writable jsw = jserde.serialize(stuff, rowOI);
		//System.err.println(jsw);
		return jserde.deserialize(jsw);
	}
}