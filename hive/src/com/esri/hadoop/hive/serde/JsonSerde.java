package com.esri.hadoop.hive.serde;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
// Hive-0.10 ~ serdeConstants ; Hive-0.9 ~ Constants - reflection below
//import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;

public class JsonSerde implements SerDe {
	
	static final Log LOG = LogFactory.getLog(JsonSerde.class.getName());
	
	StructObjectInspector rowOI;
	ArrayList<Writable> row;
	
	int numColumns;
	int geometryColumn = -1;
	ArrayList<String> columnNames;
	
	
	static JsonFactory jsonFactory = new JsonFactory();
	static String columnNameConstant = null;
	static String columnTypeConstant = null;

	
	// we want to set up the factory only once, so we'll do it in a static constructor
	static {

		// Set up the column name constants for handling both Hive 0.9 & 0.10
		Class<?> cl;
		try {      // hive-0.10
			cl = Class.forName("org.apache.hadoop.hive.serde.serdeConstants");
		} catch (ClassNotFoundException e) {
			try {  // hive-0.9
				cl = Class.forName("org.apache.hadoop.hive.serde.Constants");
			} catch (ClassNotFoundException x) {
				cl = null;
			} 
		}
		if (cl != null) {
			try {
				Object constantClass = cl.newInstance();
				columnNameConstant = (String)cl.getField("LIST_COLUMNS").get(constantClass);
				columnTypeConstant = (String)cl.getField("LIST_COLUMN_TYPES").get(constantClass);

			} catch (Exception e) {  // InstantiationException, IllegalAccessException;
				// remain null       // IllegalArgumentException, SecurityException, ...
			}                        // ... IllegalAccessException, NoSuchFieldException
		}
	}
	
	@Override
	public void initialize(Configuration arg0, Properties tbl)
			throws SerDeException {

	    // We can get the table definition from tbl.

	    // Read the configuration parameters
		if (columnNameConstant == null || columnTypeConstant == null) {
			String blame = (columnNameConstant == null ? "name" : "type");
			throw new SerDeException("Internal error regarding constant for " + blame);
		}
		String columnNameProperty = tbl.getProperty(columnNameConstant);
		String columnTypeProperty = tbl.getProperty(columnTypeConstant);

	    ArrayList<TypeInfo> typeInfos = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
	    
	    columnNames = new ArrayList<String>();
	    columnNames.addAll(Arrays.asList(columnNameProperty.split(",")));

	    numColumns = columnNames.size();

	    ArrayList<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
	        columnNames.size());
	    
	    for (int c = 0; c < numColumns; c++) {
	      if (typeInfos.get(c).equals(TypeInfoFactory.binaryTypeInfo)){
	    	  
	    	  if (geometryColumn >= 0){
	    		  // only one column can be defined as binary for geometries
	    		  throw new SerDeException("Multiple binary columns defined.  Define only one binary column for geometries");
	    	  }
	    	  
	    	  columnOIs.add(GeometryUtils.geometryTransportObjectInspector);
	    	  geometryColumn = c;
	      } else {
	    	  columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	      }
	    }
	    
	    // standardStruct uses ArrayList to store the row.
	    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
	        columnNames, columnOIs);

	    // constructing the row object, etc, which will be reused for all rows.
	    row = new ArrayList<Writable>(numColumns);
	    for (int c = 0; c < numColumns; c++) {
	      row.add(null);
	    }

	}
	
	@Override
	public Object deserialize(Writable json_in) throws SerDeException {
		Text json = (Text)json_in;
		
		try {
			JsonParser parser = jsonFactory.createJsonParser(json.toString());
			
			JsonToken token = parser.nextToken();
			
			while (token != null){
				
				if (token == JsonToken.START_OBJECT){
					if (parser.getCurrentName() == "geometry"){
						if (geometryColumn > -1){
							// create geometry and insert into geometry field
							Geometry geometry =  GeometryEngine.jsonToGeometry(parser).getGeometry();
							row.set(geometryColumn, GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(geometry, null)));
						} else {
							// no geometry in select field set, don't even bother parsing
							parser.skipChildren();
						}
					} else if (parser.getCurrentName() == "attributes"){
						
						token = parser.nextToken();
						
						while (token != JsonToken.END_OBJECT && token != null){
							
							String name = parser.getText().toLowerCase();
							
							parser.nextToken();
							
							int fieldIndex = columnNames.indexOf(name);
							
							if (fieldIndex >= 0){
								String value = parser.getText();
								row.set(fieldIndex, new Text(value));
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

		StandardStructObjectInspector structOI = (StandardStructObjectInspector)oi;
		
		// get list of writables, one for each field in the row
		List<Object> fieldWritables = structOI.getStructFieldsDataAsList(obj);
		
		StringWriter writer = new StringWriter();

		try {
			JsonGenerator jsonGen = jsonFactory.createJsonGenerator(writer);
			
			jsonGen.writeStartObject();
			
			// first write attributes
			jsonGen.writeObjectFieldStart("attributes");

			for (int i=0;i<fieldWritables.size();i++){
				if (i == geometryColumn) continue; // skip geometry, it comes later
				
				Writable writable = (Writable)fieldWritables.get(i);
				
				jsonGen.writeObjectField(columnNames.get(i), writable.toString());
			}
			
			jsonGen.writeEndObject();
			
			// if geometry column exists, write it
			if (geometryColumn > -1){
				BytesWritable bytesWritable = (BytesWritable)fieldWritables.get(geometryColumn);
				
				OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(bytesWritable);
				
				jsonGen.writeRaw(",\"geometry\":" + GeometryEngine.geometryToJson(null, ogcGeometry.getEsriGeometry()));
				
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
