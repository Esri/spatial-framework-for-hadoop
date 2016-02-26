package com.esri.hadoop.hive.serde;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.*;

import com.esri.core.geometry.Point;
import com.esri.hadoop.hive.GeometryUtils;
import com.esri.hadoop.shims.HiveShims;

// Ideally tests to cover:
//  - attributes and/or geometry
//  - null attributes and values to not linger
//  - null geometry
//  - spatial reference preserved

public class TestEsriJsonSerDe {

	@Test
	public void TestIntOnly() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		SerDe jserde = new JsonSerde();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"attributes\":{\"num\":7},\"geometry\":null}");
        value.set("{\"attributes\":{\"num\":7}}");
		Object row = jserde.deserialize(value);
		StructField f0 = rowOI.getStructFieldRef("num");
		Object fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(7, ((IntWritable)fieldData).get());
        value.set("{\"attributes\":{\"num\":9}}");
        row = jserde.deserialize(value);
		f0 = rowOI.getStructFieldRef("num");
		fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(9, ((IntWritable)fieldData).get());
	}

	@Test
	public void TestPointOnly() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		SerDe jserde = new JsonSerde();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        value.set("{\"attributes\":{},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
		Object row = jserde.deserialize(value);
		StructField f0 = rowOI.getStructFieldRef("shape");
		Object fieldData = rowOI.getStructFieldData(row, f0);
		GeometryUtils.geometryFromEsriShape((BytesWritable)fieldData).equals(new Point(15.0, 5.0));
        value.set("{\"attributes\":{},\"geometry\":{\"x\":7.0,\"y\":4.0}}");
        row = jserde.deserialize(value);
		f0 = rowOI.getStructFieldRef("shape");
		fieldData = rowOI.getStructFieldData(row, f0);
		GeometryUtils.geometryFromEsriShape((BytesWritable)fieldData).equals(new Point(7.0, 4.0));
	}

	@Test
	public void TestIntPoint() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		SerDe jserde = new JsonSerde();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num,shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "bigint,binary");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        value.set("{\"attributes\":{\"num\":7},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
		Object row = jserde.deserialize(value);
		StructField fref = rowOI.getStructFieldRef("num");
		Object fieldData = rowOI.getStructFieldData(row, fref);
		Assert.assertEquals(7, ((LongWritable)fieldData).get());

        value.set("{\"attributes\":{\"num\":4},\"geometry\":{\"x\":7.0,\"y\":2.0}}");
        row = jserde.deserialize(value);
		fref = rowOI.getStructFieldRef("num");
		fieldData = rowOI.getStructFieldData(row, fref);
		Assert.assertEquals(4, ((LongWritable)fieldData).get());
		fref = rowOI.getStructFieldRef("shape");
		fieldData = rowOI.getStructFieldData(row, fref);
		GeometryUtils.geometryFromEsriShape((BytesWritable)fieldData).equals(new Point(7.0, 2.0));
	}

	@Test
	public void TestNullAttr() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		SerDe jserde = new JsonSerde();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"attributes\":{\"num\":7},\"geometry\":null}");
        value.set("{\"attributes\":{\"num\":7}}");
		Object row = jserde.deserialize(value);
		StructField f0 = rowOI.getStructFieldRef("num");
		Object fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(7, ((IntWritable)fieldData).get());
        value.set("{\"attributes\":{}}");
        row = jserde.deserialize(value);
		f0 = rowOI.getStructFieldRef("num");
		fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(null, fieldData);
	}

	@Test
	public void TestNullGeom() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		SerDe jserde = new JsonSerde();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        value.set("{\"attributes\":{},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
		Object row = jserde.deserialize(value);
		StructField f0 = rowOI.getStructFieldRef("shape");
		Object fieldData = rowOI.getStructFieldData(row, f0);
		GeometryUtils.geometryFromEsriShape((BytesWritable)fieldData).equals(new Point(15.0, 5.0));

        value.set("{\"attributes\":{},\"geometry\":null}");
        row = jserde.deserialize(value);
		f0 = rowOI.getStructFieldRef("shape");
		fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(null, fieldData);
	}

}