package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;

import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_Overlaps",
	value = "_FUNC_(geometry1, geometry2) - return true if geometry1 overlaps geometry2",
	extended = "Example:\n" + 
	"SELECT _FUNC_(st_polygon(2,0, 2,3, 3,0), st_polygon(1,1, 1,4, 4,4, 4,1)) from src LIMIT 1;  -- return true\n" + 
	"SELECT _FUNC_(st_polygon(2,0, 2,1, 3,1), ST_Polygon(1,1, 1,4, 4,4, 4,1)) from src LIMIT 1;  -- return false"	
	)

public class ST_Overlaps extends ST_GeometryRelational {

	static final Log LOG = LogFactory.getLog(ST_Overlaps.class.getName());
	final BooleanWritable resultBoolean = new BooleanWritable();
	
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

		resultBoolean.set(ogcGeom1.overlaps(ogcGeom2));
		return resultBoolean;
	}

}
