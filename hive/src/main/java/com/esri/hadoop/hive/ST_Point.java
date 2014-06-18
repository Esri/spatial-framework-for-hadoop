package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;


import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_Point",
	value = "_FUNC_(x, y) - constructor for 2D point\n" +
    "_FUNC_('point (x y)') - constructor for 2D point",
	extended = "Example:\n" +
	"  SELECT _FUNC_(longitude, latitude) from src LIMIT 1;\n" + 
	"  SELECT _FUNC_('point (0 0)') from src LIMIT 1;")
//@HivePdkUnitTests(
//	cases = {
//		@HivePdkUnitTest(
//			query = "select ST_GeometryType(ST_Point('point (10.02 20.01)')) from onerow",
//			result = "ST_POINT"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Equals(ST_Point('point (10.02 20.01)'), ST_GeomFromText('point (10.02 20.01)')) from onerow",
//			result = "true"
//			)
//		}
//	)

public class ST_Point extends ST_Geometry {
	static final Log LOG = LogFactory.getLog(ST_Point.class.getName());

	// Number-pair constructor - 2D
	public BytesWritable evaluate(DoubleWritable x, DoubleWritable y) {
		return evaluate(x, y, null, null);
	}

	// Number-triplet constructor - 3D
	public BytesWritable evaluate(DoubleWritable x, DoubleWritable y, DoubleWritable z) {
		return evaluate(x, y, z, null);
	}

	// Number-list constructor - ZM
	public BytesWritable evaluate(DoubleWritable x, DoubleWritable y, DoubleWritable z, DoubleWritable m) {
		if (x == null || y == null) {
			//LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}
		try {
			Point stPt = new Point(x.get(), y.get());
			if (z != null)
				stPt.setZ(z.get());
			if (m != null)
				stPt.setM(m.get());
			BytesWritable ret = GeometryUtils.geometryToEsriShapeBytesWritable(OGCGeometry.createFromEsriGeometry(stPt, null));
			return ret;
		} catch (Exception e) {
		    //LogUtils.Log_InternalError(LOG, "ST_Point: " + e);
		    return null;
		}
	}

	// WKT constructor - can use SetSRID on constructed point
	public BytesWritable evaluate(Text wkwrap) throws UDFArgumentException {
		String wkt = wkwrap.toString();
		try {
			OGCGeometry ogcObj = OGCGeometry.fromText(wkt);
			ogcObj.setSpatialReference(null);
			if (ogcObj.geometryType().equals("Point")) {
				return GeometryUtils.geometryToEsriShapeBytesWritable(ogcObj);
			} else {
				LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_POINT, GeometryUtils.OGCType.UNKNOWN);
				return null;
			}

		} catch (Exception e) {  // IllegalArgumentException, GeometryException
			LogUtils.Log_InvalidText(LOG, wkt);
			return null;
		}
	}
}
