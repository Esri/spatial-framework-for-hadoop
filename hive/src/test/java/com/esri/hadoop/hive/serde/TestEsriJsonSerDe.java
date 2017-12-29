package com.esri.hadoop.hive.serde;

import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.shims.HiveShims;

// Ideally tests to cover:
//  - attributes and/or geometry
//  - null attributes and values to not linger
//  - null geometry
//  - spatial reference preserved

public class TestEsriJsonSerDe extends JsonSerDeTestingBase {

	@Test
	public void TestIntWrite() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
		AbstractSerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        // {"attributes":{"num":7}}
        addWritable(stuff, 7);
		Writable jsw = jserde.serialize(stuff, rowOI);
		JsonNode jn = new ObjectMapper().readTree(((Text)jsw).toString());
		jn = jn.findValue("attributes");
		jn = jn.findValue("num");
		Assert.assertEquals(7, jn.getIntValue());
	}

	@Test
	public void TestEpochWrite() throws Exception {
		ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "date");
		AbstractSerDe jserde = mkSerDe(proptab);
		StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

		// {"attributes":{"when":147147147147}}
		long epoch = 147147147147L;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd");
		sdf.setTimeZone(TimeZone.getTimeZone("America/New_York"));
		java.sql.Date expected = new java.sql.Date(epoch);
		String expString = sdf.format(expected);
		//System.err.println(expected.getTime());
		addWritable(stuff, expected);
		Writable jsw = jserde.serialize(stuff, rowOI);
		JsonNode jn = new ObjectMapper().readTree(((Text)jsw).toString());
		jn = jn.findValue("attributes");
		jn = jn.findValue("when");
		java.sql.Date actual = new java.sql.Date(jn.getLongValue());
		String actualDateString = sdf.format(actual);
		Assert.assertEquals(expString, actualDateString);  // workaround DateWritable,j.s.Date
	}
	@Test
	public void TestTimeWrite() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "timestamp");
		AbstractSerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        // {"attributes":{"when":147147147147}}
        long epoch = 147147147147L;
		java.sql.Timestamp expected = new java.sql.Timestamp(epoch);
        addWritable(stuff, expected);
		Writable jsw = jserde.serialize(stuff, rowOI);
		JsonNode jn = new ObjectMapper().readTree(((Text)jsw).toString());
		jn = jn.findValue("attributes");
		jn = jn.findValue("when");
		java.sql.Timestamp actual = new java.sql.Timestamp(jn.getLongValue());
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void TestPointWrite() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
		AbstractSerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        // {"attributes":{},"geometry":{"x":15.0,"y":5.0}}
        addWritable(stuff, new Point(15.0, 5.0));
		Writable jsw = jserde.serialize(stuff, rowOI);
        String rslt = ((Text)jsw).toString();
		JsonNode jn = new ObjectMapper().readTree(rslt);
		jn = jn.findValue("geometry");
		Assert.assertNotNull(jn.findValue("x"));
		Assert.assertNotNull(jn.findValue("y"));
	}

	@Test
	public void TestIntParse() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		AbstractSerDe jserde = new EsriJsonSerDe();
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
	public void TestDateParse() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		AbstractSerDe jserde = new EsriJsonSerDe();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "date");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        value.set("{\"attributes\":{\"when\":\"2020-02-20\"}}");
		Object row = jserde.deserialize(value);
		StructField f0 = rowOI.getStructFieldRef("when");
		Object fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals("2020-02-20",
							((DateWritable)fieldData).get().toString());
        value.set("{\"attributes\":{\"when\":\"2017-05-05\"}}");
        row = jserde.deserialize(value);
		fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals("2017-05-05",
							((DateWritable)fieldData).get().toString());
	}

	@Test
	public void TestEpochParse() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		AbstractSerDe jserde = new EsriJsonSerDe();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "date");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        value.set("{\"attributes\":{\"when\":147147147147}}");
		Object row = jserde.deserialize(value);
		StructField f0 = rowOI.getStructFieldRef("when");
		Object fieldData = rowOI.getStructFieldData(row, f0);
		//Assert.assertEquals(147147147147L, ((DateWritable)fieldData).get().getTime());
		Assert.assertEquals(new java.sql.Date(147147147147L).toString(),
							((DateWritable)fieldData).get().toString());
        value.set("{\"attributes\":{\"when\":142857142857}}");
        row = jserde.deserialize(value);
		fieldData = rowOI.getStructFieldData(row, f0);
		//Assert.assertEquals(142857142857L, ((DateWritable)fieldData).get());
		Assert.assertEquals(new java.sql.Date(142857142857L).toString(),
							((DateWritable)fieldData).get().toString());
	}

	@Test
	public void TestTimeParse() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		AbstractSerDe jserde = new EsriJsonSerDe();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "when");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "timestamp");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        value.set("{\"attributes\":{\"when\":\"2020-02-20\"}}");
		Object row = jserde.deserialize(value);
		StructField f0 = rowOI.getStructFieldRef("when");
		Object fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(
			new java.text.SimpleDateFormat("yyyy-MM-dd").parse("2020-02-20").getTime(),
			((TimestampWritable)fieldData).getTimestamp().getTime());
        value.set("{\"attributes\":{\"when\":\"2017-05-05 05:05\"}}");
        row = jserde.deserialize(value);
		fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(
			new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm").parse("2017-05-05 05:05").getTime(),
			((TimestampWritable)fieldData).getTimestamp().getTime());
        value.set("{\"attributes\":{\"when\":\"2017-08-09 10:11:12\"}}");
        row = jserde.deserialize(value);
		fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(
		  new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2017-08-09 10:11:12").getTime(),
		  ((TimestampWritable)fieldData).getTimestamp().getTime());
        value.set("{\"attributes\":{\"when\":\"2017-06-05 04:03:02.123456789\"}}");
        row = jserde.deserialize(value);
		fieldData = rowOI.getStructFieldData(row, f0);
		Assert.assertEquals(
		  new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse("2017-06-05 04:03:02.123").getTime(),
		  ((TimestampWritable)fieldData).getTimestamp().getTime());  // ns parsed but not checked
	}

	@Test
	public void TestPointParse() throws Exception {
		Configuration config = new Configuration();
		Text value = new Text();

		AbstractSerDe jserde = new EsriJsonSerDe();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "binary");
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        value.set("{\"attributes\":{},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
		Object row = jserde.deserialize(value);
		StructField f0 = rowOI.getStructFieldRef("shape");
		Object fieldData = rowOI.getStructFieldData(row, f0);
		ckPoint(new Point(15.0, 5.0), (BytesWritable)fieldData);

        value.set("{\"attributes\":{},\"geometry\":{\"x\":7.0,\"y\":4.0}}");
        row = jserde.deserialize(value);
		f0 = rowOI.getStructFieldRef("shape");
		fieldData = rowOI.getStructFieldData(row, f0);
		ckPoint(new Point(7.0, 4.0), (BytesWritable)fieldData);
	}

	@Test
	public void TestIntOnly() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "int");
		AbstractSerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        addWritable(stuff, 7);
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("num", row, rowOI);
		Assert.assertEquals(7, ((IntWritable)fieldData).get());
        // value.set("{\"attributes\":{\"num\":9}}");
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
		AbstractSerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"attributes\":{},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
        addWritable(stuff, new Point(15.0, 5.0));
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("shape", row, rowOI);
		ckPoint(new Point(15.0, 5.0), (BytesWritable)fieldData);

        //value.set("{\"attributes\":{},\"geometry\":{\"x\":7.0,\"y\":4.0}}");
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
		AbstractSerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"attributes\":{\"num\":7},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
        addWritable(stuff, 7L);
        addWritable(stuff, new Point(15.0, 5.0));
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("num", row, rowOI);
		Assert.assertEquals(7, ((LongWritable)fieldData).get());

        //value.set("{\"attributes\":{\"num\":4},\"geometry\":{\"x\":7.0,\"y\":2.0}}");
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
		AbstractSerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"attributes\":{\"num\":7}}");
		addWritable(stuff, 7);
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("num", row, rowOI);
		Assert.assertEquals(7, ((IntWritable)fieldData).get());
        //value.set("{\"attributes\":{}}");
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
		AbstractSerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"attributes\":{},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
        addWritable(stuff, new Point(15.0, 5.0));
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("shape", row, rowOI);
		ckPoint(new Point(15.0, 5.0), (BytesWritable)fieldData);

        //value.set("{\"attributes\":{},\"geometry\":null}");
		stuff.set(0, null);
		row = runSerDe(stuff, jserde, rowOI);
		fieldData = getField("shape", row, rowOI);
		Assert.assertNull(fieldData);
	}

	@Test
	public void TestColumnTypes() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "flag,num1,num2,text");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "boolean,tinyint,smallint,string");
		AbstractSerDe jserde = mkSerDe(proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

		// {"attributes":{"flag":false,"num":"5","text":"Point(15.0 5.0)"}}
        addWritable(stuff, false);
        addWritable(stuff, (byte)2);
        addWritable(stuff, (short)5);
        addWritable(stuff, "Point(15.0 5.0)");
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("flag", row, rowOI);
		Assert.assertEquals(false, ((BooleanWritable)fieldData).get());
		fieldData = getField("num1", row, rowOI);
		Assert.assertEquals((byte)2, ((ByteWritable)fieldData).get());
		fieldData = getField("num2", row, rowOI);
		Assert.assertEquals((short)5, ((ShortWritable)fieldData).get());
		fieldData = getField("text", row, rowOI);
		Assert.assertEquals("Point(15.0 5.0)", ((Text)fieldData).toString());

		stuff.set(0, new BooleanWritable(true));
		stuff.set(1, new ByteWritable((byte)4));
		stuff.set(2, new ShortWritable((short)4));
		//stuff.set(3, new Text("other"));
		LazyStringObjectInspector loi = LazyPrimitiveObjectInspectorFactory.
			getLazyStringObjectInspector(false, (byte)'\0');
		LazyString lstr = new LazyString(loi);
		ByteArrayRef bar = new ByteArrayRef();
		bar.setData("other".getBytes());
		lstr.init(bar, 0, 5);
		stuff.set(3, lstr);
		row = runSerDe(stuff, jserde, rowOI);
		fieldData = getField("flag", row, rowOI);
		Assert.assertEquals(true, ((BooleanWritable)fieldData).get());
		fieldData = getField("num1", row, rowOI);
		Assert.assertEquals((byte)4, ((ByteWritable)fieldData).get());
		fieldData = getField("num2", row, rowOI);
		Assert.assertEquals((short)4, ((ShortWritable)fieldData).get());
		fieldData = getField("text", row, rowOI);
		Assert.assertEquals("other", ((Text)fieldData).toString());
	}
    /* *
	@Deprecated -> Obsolete
	@Test
	public void LegacyName() throws Exception {
        ArrayList<Object> stuff = new ArrayList<Object>();
		Properties proptab = new Properties();
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMNS, "num,shape");
		proptab.setProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES, "bigint,binary");
		Configuration config = new Configuration();
		AbstractSerDe jserde = new JsonSerde();
		jserde.initialize(config, proptab);
        StructObjectInspector rowOI = (StructObjectInspector)jserde.getObjectInspector();

        //value.set("{\"attributes\":{\"num\":7},\"geometry\":{\"x\":15.0,\"y\":5.0}}");
        addWritable(stuff, 7L);
        addWritable(stuff, new Point(15.0, 5.0));
		Object row = runSerDe(stuff, jserde, rowOI);
		Object fieldData = getField("num", row, rowOI);
		Assert.assertEquals(7, ((LongWritable)fieldData).get());

        //value.set("{\"attributes\":{\"num\":4},\"geometry\":{\"x\":7.0,\"y\":2.0}}");
		stuff.clear();
        addWritable(stuff, 4L);
        addWritable(stuff, new Point(7.0, 2.0));
		row = runSerDe(stuff, jserde, rowOI);
		fieldData = getField("num", row, rowOI);
		Assert.assertEquals(4, ((LongWritable)fieldData).get());
		fieldData = getField("shape", row, rowOI);
		ckPoint(new Point(7.0, 2.0), (BytesWritable)fieldData);
	}
    * */

	private AbstractSerDe mkSerDe(Properties proptab) throws Exception {
		Configuration config = new Configuration();
		AbstractSerDe jserde = new EsriJsonSerDe();
		jserde.initialize(config, proptab);
		return jserde;
	}

}
