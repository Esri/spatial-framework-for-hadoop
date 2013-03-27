package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;

import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_Buffer",
	value = "_FUNC_(ST_Geometry, distance) - ST_Geometry buffered by distance",
	extended = "Example:\n"
	+ "  SELECT _FUNC_(ST_Point(0, 0), 1) FROM src LIMIT 1;   -- polygon approximating a unit circle\n"
	)
public class ST_Buffer extends ST_GeometryProcessing {

	static final Log LOG = LogFactory.getLog(ST_Buffer.class.getName());

	public BytesWritable evaluate(BytesWritable geometryref1, DoubleWritable distance)
	{
		if (geometryref1 == null || geometryref1.getLength() == 0 || distance == null) {
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geometryref1);
		if (ogcGeometry == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry bufferedGeometry = ogcGeometry.buffer(distance.get());
		// TODO persist type information (polygon vs multipolygon)
		return GeometryUtils.geometryToEsriShapeBytesWritable(bufferedGeometry);
	}
}
