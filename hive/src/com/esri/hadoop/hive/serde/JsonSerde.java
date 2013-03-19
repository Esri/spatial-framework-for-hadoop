package com.esri.hadoop.hive.serde;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
// Hive-0.10 ~ serdeConstants ; Hive-0.9 ~ Constants - reflection below
//import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.module.SimpleModule;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils;
//import com.esri.hadoop.hive.HiveGeometry;


public class JsonSerde implements SerDe {
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
	    ObjectMapper mapper = new ObjectMapper();
		SimpleModule module = new SimpleModule("EsriDeserializers", new Version(1, 0, 0, null));

		// add deserializers for types that can't be mapped field for field from the JSON
		module.addDeserializer(Geometry.class, new GeometryJsonDeserializer());
		
		mapper.registerModule(module);
		
		jsonFactory.setCodec(mapper);

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
			
			EsriFeature feature = parser.readValueAs(EsriFeature.class);
			
			for (int i=0;i<columnNames.size();i++){
				if (i == geometryColumn)
				{
					OGCGeometry ogcObj = OGCGeometry.createFromEsriGeometry(feature.geometry, null);
					row.set(i, GeometryUtils.geometryToEsriShapeBytesWritable(ogcObj));
				} else {
					Object val = feature.attributes.get(columnNames.get(i));
					
					if (val != null){
						row.set(i, new Text(val.toString()));
					} else {
						row.set(i, new Text("???"));
					}
				}
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
	public Writable serialize(Object arg0, ObjectInspector arg1)
			throws SerDeException {
		return (Text)arg0;
	}
	
	private static class EsriFeature {
		/**
		* Map of attributes
		*/
		public CaseInsensitiveMap attributes;

		/**
		* Geometry associated with this feature
		*/
		public Geometry geometry;
	}
	
	private static class GeometryJsonDeserializer extends JsonDeserializer<Geometry> {

		public GeometryJsonDeserializer(){}

		@Override
		public Geometry deserialize(JsonParser arg0, DeserializationContext arg1)
		throws IOException, JsonProcessingException {
		return GeometryEngine.jsonToGeometry(arg0).getGeometry();
		}
	}
	
	@SuppressWarnings("serial")
	public static class CaseInsensitiveMap extends HashMap<String, Object> {

		public CaseInsensitiveMap(){
			
		}
		
	    public Object put(String key, Object value) {
	       return super.put(key.toLowerCase(), value);
	    }

	    public Object get(String key) {
	       return super.get(key.toLowerCase());
	    }
	}
}
