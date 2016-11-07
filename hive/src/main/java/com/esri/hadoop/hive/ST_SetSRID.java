package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;

import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(name = "ST_SetSRID",
value = "_FUNC_(<ST_Geometry>, SRID) - set the Spatial Reference ID of the geometry",
extended = "Example:\n"
+ "  > SELECT _FUNC_(ST_SetSRID(ST_Point(1.5, 2.5), 4326)) FROM src LIMIT 1;\n"
+ "  -- create a point and then set its SRID to 4326"
)

public class ST_SetSRID extends ST_Geometry {
	static final Log LOG = LogFactory.getLog(ST_SetSRID.class.getName());
	
	public BytesWritable evaluate(BytesWritable geomref, IntWritable wkwrap){
		if (geomref == null || geomref.getLength() == 0){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		// just return the geometry ref without setting anything if wkid is null
		if (wkwrap == null){
			return geomref;
		}

		int wkid = wkwrap.get();
		if (GeometryUtils.getWKID(geomref) != wkid) {
			GeometryUtils.setWKID(geomref, wkid);
		}

		return geomref;
	}
}
