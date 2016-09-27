package com.esri.hadoop.hive;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;


import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_MultiPolygon",
	value = "_FUNC_(array(x1, y1, x2, y2, ... ), array(x1, y1, x2, y2, ... ), ... ) - constructor for 2D multi polygon\n" +
    "_FUNC_('multipolygon ( ... )') - constructor for 2D multi polygon",
	extended = "Example:\n" +
	"  SELECT _FUNC_(array(1, 1, 1, 2, 2, 2, 2, 1), array(3, 3, 3, 4, 4, 4, 4, 3)) from src LIMIT 1;\n" + 
	"  SELECT _FUNC_('multipolygon (((0 0, 0 1, 1 0, 0 0)), ((2 2, 2 3, 3 2, 2 2)))') from src LIMIT 1;")
//@HivePdkUnitTests(
//	cases = { 
//		@HivePdkUnitTest(
//			query = "select st_asjson(st_multipolygon(array(1, 1, 1, 2, 2, 2, 2, 1), array(3, 3, 3, 4, 4, 4, 4, 3))) from onerow;",
//			result = "{\"rings\":[[[1.0,1.0],[1.0,2.0],[2.0,2.0],[2.0,1.0],[1.0,1.0]],[[3.0,3.0],[3.0,4.0],[4.0,4.0],[4.0,3.0],[3.0,3.0]]]}"
//			),
//		@HivePdkUnitTest(
//			query = "select ST_Equals(ST_MultiPolygon('multipolygon (((0 0, 0 1, 1 0, 0 0)), ((2 2, 2 3, 3 2, 2 2)))'), ST_GeomCollection('multipolygon (((0 0, 0 1, 1 0, 0 0)), ((2 2, 2 3, 3 2, 2 2)))')) from onerow",
//			result = "true"
//			)
//		}
//)
public class ST_MultiPolygon extends ST_Geometry {
	
	static final Log LOG = LogFactory.getLog(ST_MultiPolygon.class.getName());
	
	// Number-pairs constructor
	public BytesWritable evaluate(List<DoubleWritable> ... multipaths) throws UDFArgumentLengthException{

		if (multipaths == null || multipaths.length == 0) {
			LogUtils.Log_ArgumentsNull(LOG);
			return null;
		}

		try {
			String wkt = "multipolygon(";
			int arg_idx=0;
			String comma = "";  // comma except first time

			for (List<DoubleWritable> multipath : multipaths) {
				int len = multipath.size();
				if (len < 6 || len %2 != 0) {
					LogUtils.Log_VariableArgumentLengthXY(LOG, arg_idx);
					return null;
				}

				double xStart = multipath.get(0).get(), yStart = multipath.get(1).get();
				wkt += comma + "((" + xStart + " " + yStart;

				int ix;  // index persists after loop
				for (ix = 2; ix < len; ix += 2) {
					wkt += ", " + multipath.get(ix) + " " + multipath.get(ix+1);
				}
				double xEnd = multipath.get(ix-2).get(), yEnd = multipath.get(ix-1).get();
				// This counts on the same string getting parsed to double exactly equally
				if (xEnd != xStart || yEnd != yStart)
					wkt += ", " + xStart + " " + yStart;  // close the ring

				wkt += "))";
				comma = ",";
				arg_idx++;
			}
			wkt += ")";

			return evaluate(new Text(wkt));
		} catch (Exception e) {
		    LogUtils.Log_InternalError(LOG, "ST_MultiPolygon: " + e);
		    return null;
		}
	}

	// WKT constructor - can use SetSRID on constructed multi-polygon
	public BytesWritable evaluate(Text wkwrap) throws UDFArgumentException {
		String wkt = wkwrap.toString();
		try {
			OGCGeometry ogcObj = OGCGeometry.fromText(wkt);
			ogcObj.setSpatialReference(null);
			if (ogcObj.geometryType().equals("MultiPolygon")) {
				return GeometryUtils.geometryToEsriShapeBytesWritable(ogcObj);
			} else {
				LogUtils.Log_InvalidType(LOG, GeometryUtils.OGCType.ST_MULTIPOLYGON, GeometryUtils.OGCType.UNKNOWN);
				return null;
			}
		} catch (Exception e) {  // IllegalArgumentException, GeometryException
			LogUtils.Log_InvalidText(LOG, wkt);
			return null;
		}
	}

}
