package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;

import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_Equals",
	value = "_FUNC_(geometry1, geometry2) - return true if geometry1 equals geometry2",
	extended = "Example:\n" + 
	"SELECT _FUNC_(st_linestring(0,0, 1,1), st_linestring(1,1, 0,0)) from src LIMIT 1;  -- return true\n" + 
	"SELECT _FUNC_(st_linestring(0,0, 1,1), st_linestring(1,0, 0,1)) from src LIMIT 1;  -- return false\n"	
	)

public class ST_Equals extends ST_GeometryRelational {

	static final Log LOG = LogFactory.getLog(ST_Equals.class.getName());
	public final BooleanWritable resultBoolean = new BooleanWritable();
	
	public BooleanWritable evaluate(BytesWritable geometryref1, BytesWritable geometryref2)
	{
		if (geometryref1 == null || geometryref2 == null ||
		    geometryref1.getLength() == 0 || geometryref2.getLength() == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}
		
		if (!GeometryUtils.compareSpatialReferences(geometryref1, geometryref2)) {
			LogUtils.Log_SRIDMismatch(LOG, geometryref1, geometryref2);
			return null;
		}

		OGCGeometry ogcGeom1 = GeometryUtils.geometryFromEsriShape(geometryref1);
		OGCGeometry ogcGeom2 = GeometryUtils.geometryFromEsriShape(geometryref2);
		if (ogcGeom1 == null || ogcGeom2 == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		resultBoolean.set(ogcGeom1.equals(ogcGeom2));
		return resultBoolean;
	}

}
