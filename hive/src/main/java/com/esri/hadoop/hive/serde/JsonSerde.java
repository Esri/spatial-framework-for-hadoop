package com.esri.hadoop.hive.serde;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;

import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
import com.esri.hadoop.shims.HiveShims;

public class JsonSerde implements SerDe {

	static final Log LOG = LogFactory.getLog(JsonSerde.class.getName());

	static JsonFactory jsonFactory = new JsonFactory();
	static String columnNameConstant = null;
	static String columnTypeConstant = null;

	StructObjectInspector rowOI; // contains the type information for the fields returned
	
	/* rowBase keeps a base copy of the Writable for each field so they can be reused for 
	 * all records. When deserialize is called, row is initially nulled out. Then for each attribute
	 * found in the JSON record the Writable reference is copied from rowBase to row
	 * and set to the appropriate value.  Then row is returned.  This why values don't linger from 
	 * previous records.
	 */
	ArrayList<Writable> rowBase; 
	ArrayList<Writable> row;

	int numColumns;
	int geometryColumn = -1;
	ArrayList<String> columnNames;
	ArrayList<ObjectInspector> columnOIs;
	
	boolean [] columnSet; 
	
	@Override
	public void initialize(Configuration arg0, Properties tbl)
			throws SerDeException {
				
		geometryColumn = -1;

	    // Read the configuration parameters
		String columnNameProperty = tbl.getProperty(HiveShims.serdeConstants.LIST_COLUMNS);
		String columnTypeProperty = tbl.getProperty(HiveShims.serdeConstants.LIST_COLUMN_TYPES);

		ArrayList<TypeInfo> typeInfos = TypeInfoUtils
				.getTypeInfosFromTypeString(columnTypeProperty);

		columnNames = new ArrayList<String>();
		columnNames.addAll(Arrays.asList(columnNameProperty.toLowerCase().split(",")));

		numColumns = columnNames.size();
		
		columnOIs = new ArrayList<ObjectInspector>(numColumns);
		columnSet = new boolean[numColumns];
		
		for (int c = 0; c < numColumns; c++) {

			TypeInfo colTypeInfo = typeInfos.get(c);
			
			if (colTypeInfo.getCategory() != Category.PRIMITIVE){
				throw new SerDeException("Only primitive field types are accepted");
			}
			
			if (colTypeInfo.getTypeName().equals("binary")) {

				if (geometryColumn >= 0) {
					// only one column can be defined as binary for geometries
					throw new SerDeException(
							"Multiple binary columns defined.  Define only one binary column for geometries");
				}

				columnOIs.add(GeometryUtils.geometryTransportObjectInspector);
				geometryColumn = c;
			} else {
				columnOIs.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(colTypeInfo));
			}
		}

		// standardStruct uses ArrayList to store the row.
		rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
				columnNames, columnOIs);

		// constructing the row objects, etc, which will be reused for all rows.
		rowBase = new ArrayList<Writable>(numColumns);
		row = new ArrayList<Writable>(numColumns);
		
		// set each value in rowBase to the writable that corresponds with its PrimitiveObjectInspector
		for (int c = 0; c < numColumns; c++) {
			
			PrimitiveObjectInspector poi = (PrimitiveObjectInspector)columnOIs.get(c);
			Writable writable;
			
			try {
				writable = (Writable)poi.getPrimitiveWritableClass().newInstance();
			} catch (InstantiationException e) {
				throw new SerDeException("Error creating Writable from ObjectInspector", e);
			} catch (IllegalAccessException e) {
				throw new SerDeException("Error creating Writable from ObjectInspector", e);
			}
			
			rowBase.add(writable);
			row.add(null); // default all values to null
		}
	}
	
	/**
	 * Copies the Writable at fieldIndex from rowBase to row, then sets the value of the Writable
	 * to the value in parser
	 * 
	 * @param fieldIndex column index of field in row
	 * @param parser JsonParser pointing to the attribute
	 * @throws JsonParseException
	 * @throws IOException
	 */
	private void setRowFieldFromParser(int fieldIndex, JsonParser parser) throws JsonParseException, IOException{

		PrimitiveObjectInspector poi = (PrimitiveObjectInspector)this.columnOIs.get(fieldIndex);
		
		// set the field in the row to the writable from rowBase
		row.set(fieldIndex, rowBase.get(fieldIndex));

		switch (poi.getPrimitiveCategory()){
		case SHORT:
			((ShortWritable)row.get(fieldIndex)).set(parser.getShortValue());
			break;
		case INT:
			((IntWritable)row.get(fieldIndex)).set(parser.getIntValue());
			break;
		case LONG:
			((LongWritable)row.get(fieldIndex)).set(parser.getLongValue());
			break;
		case DOUBLE:
			((DoubleWritable)row.get(fieldIndex)).set(parser.getDoubleValue());
			break;
		case FLOAT:
			((FloatWritable)row.get(fieldIndex)).set(parser.getFloatValue());
			break;
		case BOOLEAN:
			((BooleanWritable)row.get(fieldIndex)).set(parser.getBooleanValue());
			break;
		case STRING:
			((Text)row.get(fieldIndex)).set(parser.getText());
			break;
		default:
			((Text)row.get(fieldIndex)).set(parser.getText());
			break;	
		}
	}
	
	/**
	 * Send to the generator, the value of the Writable, using column type
	 * 
	 * @param value The attribute value as a Writable
	 * @param fieldIndex column index of field in row
	 * @param jsonGen JsonGenerator
	 * @throws JsonProcessingException
	 * @throws IOException
	 */
	private void generateJsonFromValue(Writable value, int fieldIndex, JsonGenerator jsonGen)
		throws JsonProcessingException, IOException {

		if (value == null) {
			jsonGen.writeObjectField(columnNames.get(fieldIndex), null);
			return;
		}
		
		PrimitiveObjectInspector poi = (PrimitiveObjectInspector)this.columnOIs.get(fieldIndex);

		switch (poi.getPrimitiveCategory()) {
		case SHORT:
			jsonGen.writeObjectField(columnNames.get(fieldIndex), ((ShortWritable)value).get());
			break;
		case INT:
			jsonGen.writeObjectField(columnNames.get(fieldIndex), ((IntWritable)value).get());
			break;
		case LONG:
			jsonGen.writeObjectField(columnNames.get(fieldIndex), ((LongWritable)value).get());
			break;
		case DOUBLE:
			jsonGen.writeObjectField(columnNames.get(fieldIndex), ((DoubleWritable)value).get());
			break;
		case FLOAT:
			jsonGen.writeObjectField(columnNames.get(fieldIndex), ((FloatWritable)value).get());
			break;
		case BOOLEAN:
			jsonGen.writeObjectField(columnNames.get(fieldIndex), ((BooleanWritable)value).get());
			break;
		default:	/* especially:	case STRING: */
			jsonGen.writeObjectField(columnNames.get(fieldIndex), value.toString());
			break;	
		}
	}
	
	@Override
	public Object deserialize(Writable json_in) throws SerDeException {
		Text json = (Text) json_in;

		// null out array because we reuse it and we don't want values persisting
		// from the last record
		for (int i=0;i<numColumns;i++)
			row.set(i, null);
		
		try {
			JsonParser parser = jsonFactory.createJsonParser(json.toString());

			JsonToken token = parser.nextToken();

			while (token != null) {

				if (token == JsonToken.START_OBJECT) {
					if (parser.getCurrentName() == "geometry") {
						if (geometryColumn > -1) {
							// create geometry and insert into geometry field
							MapGeometry mapGeom = GeometryEngine.jsonToGeometry(parser);
							row.set(geometryColumn, mapGeom == null ? null :
									GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(mapGeom.getGeometry(),
																													  mapGeom.getSpatialReference())));
						} else {
							// no geometry in select field set, don't even bother parsing
							parser.skipChildren();
						}
					} else if (parser.getCurrentName() == "attributes") {

						token = parser.nextToken();

						while (token != JsonToken.END_OBJECT && token != null) {

							// hive makes all column names in the queries column list lower case
							String name = parser.getText().toLowerCase();

							parser.nextToken();

							// figure out which column index corresponds with the attribute name
							int fieldIndex = columnNames.indexOf(name);

							if (fieldIndex >= 0) {
								setRowFieldFromParser(fieldIndex, parser);
							} 

							token = parser.nextToken();
						}

						token = parser.nextToken();
					}
				}

				token = parser.nextToken();
			}

		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return row;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector oi)
			throws SerDeException {

		StandardStructObjectInspector structOI = (StandardStructObjectInspector) oi;

		// get list of writables, one for each field in the row
		List<Object> fieldWritables = structOI.getStructFieldsDataAsList(obj);

		StringWriter writer = new StringWriter();

		try {
			JsonGenerator jsonGen = jsonFactory.createJsonGenerator(writer);

			jsonGen.writeStartObject();

			// first write attributes
			jsonGen.writeObjectFieldStart("attributes");

			for (int i = 0; i < fieldWritables.size(); i++) {
				if (i == geometryColumn)
					continue; // skip geometry, it comes later

				Writable writable;
				Object tmpObj = fieldWritables.get(i);
				if (tmpObj instanceof LazyPrimitive<?,?>) {  // usually Text, but have seen LazyString
					writable = ((LazyPrimitive<?,?>)(tmpObj)).getWritableObject();
				} else {
					writable = (Writable)tmpObj;
				}

				try {
					generateJsonFromValue(writable, i, jsonGen);
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			jsonGen.writeEndObject();

			// if geometry column exists, write it
			if (geometryColumn > -1) {
				BytesWritable bytesWritable = (BytesWritable)fieldWritables.get(geometryColumn);
				if (bytesWritable == null) {
					jsonGen.writeObjectField("geometry", null);
				} else {
					OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(bytesWritable);
					jsonGen.writeRaw(",\"geometry\":" + GeometryEngine.geometryToJson(ogcGeometry.getEsriSpatialReference(),
																				  ogcGeometry.getEsriGeometry()));
				}				
			}

			jsonGen.writeEndObject();

			jsonGen.close();

		} catch (JsonGenerationException e) {
			LOG.error("Error generating JSON", e);
			return null;
		} catch (IOException e) {
			LOG.error("Error generating JSON", e);
			return null;
		}

		return new Text(writer.toString());
	}
}
