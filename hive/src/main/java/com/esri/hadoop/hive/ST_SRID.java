package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;


@Description(name = "ST_SRID",
value = "_FUNC_(ST_Geometry) - get the Spatial Reference ID of the geometry",
extended = "Example:\n"
+ "  SELECT _FUNC_(ST_Point(1.5, 2.5)) FROM src LIMIT 1  -- returns SRID 0"
)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_SRID(ST_SetSRID(ST_Point(1.1, 2.2), 4326)) FROM onerow",
//			result = "4326"
//		)
//	}
//)

public class ST_SRID extends ST_GeometryAccessor {
	static final Log LOG = LogFactory.getLog(ST_SRID.class.getName());
	
	IntWritable resultInt = new IntWritable();
	
	public IntWritable evaluate(BytesWritable geomref){
		if (geomref == null || geomref.getLength() == 0){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}
		
		resultInt.set(GeometryUtils.getWKID(geomref));
		return resultInt;
	}
}
