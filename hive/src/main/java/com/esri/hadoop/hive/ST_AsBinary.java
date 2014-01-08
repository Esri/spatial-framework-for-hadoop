package com.esri.hadoop.hive;

import java.nio.ByteBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;


import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_AsBinary",
	value = "_FUNC_(ST_Geometry) - return Well-Known Binary (WKB) representation of geometry\n",
	extended = "Example:\n" +
	"  SELECT _FUNC_(ST_Point(1, 2)) FROM onerow; -- WKB representation of POINT (1 2)\n"
	)
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('linestring (10 40, 40 30)')))) from onerow",
//			result = "ST_LINESTRING"
//			)
//		}
//	)

public class ST_AsBinary extends ST_Geometry {
	
	static final Log LOG = LogFactory.getLog(ST_AsBinary.class.getName());
	
	public BytesWritable evaluate(BytesWritable geomref) {
		if (geomref == null || geomref.getLength() == 0){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
		if (ogcGeometry == null){
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		try {
			ByteBuffer byteBuf = ogcGeometry.asBinary();
			byte [] byteArr = byteBuf.array();
			return new BytesWritable(byteArr);
		} catch (Exception e){
			LOG.error(e.getMessage());
			return null;
		}
	}
}
