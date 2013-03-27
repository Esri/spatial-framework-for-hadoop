package com.esri.hadoop.hive;

import java.lang.reflect.Method;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hive.pdk.HivePdkUnitTest;
import org.apache.hive.pdk.HivePdkUnitTests;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.SpatialReference;

import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_AsGeoJSON",
	value = "_FUNC_(geometry) - return GeoJSON representation of geometry\n",
	extended = "Example:\n" +
	"  SELECT _FUNC_(ST_Point(1.0, 2.0)) from onerow; -- {\"type\":\"Point\", \"coordinates\":[1.0, 2.0]}\n" +
	"Note : \n" +
	" ST_AsGeoJSON returns null, until the underlying implementation is available in geometry-api-java.\n"
	)
@HivePdkUnitTests(
	cases = { 
		@HivePdkUnitTest(
			query = "select ST_AsGeoJSON(ST_point(1, 2))) from onerow",
			result = "{\"type\":\"Point\", \"coordinates\":[1.0, 2.0]}"
			),
		@HivePdkUnitTest(
			query = "SELECT ST_AsGeoJSON(ST_MultiLineString(array(1, 1, 2, 2, 3, 3), array(10, 10, 11, 11, 12, 12))) from onerow",
			result = "todo"
			),
		@HivePdkUnitTest(
			query = "SELECT ST_AsGeoJSON(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)) from onerow",
			result = "todo"
			)
		}
	)

public class ST_AsGeoJson extends ST_Geometry {
	static final Log LOG = LogFactory.getLog(ST_AsGeoJson.class.getName());
	
	public Text evaluate(BytesWritable geomref){
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
			//throw new Exception("build updated geometry-api");
			//return new Text(GeometryEngine.geometryToGeoJson(spatialReference, esriGeom));
			Method outMethod = OGCGeometry.class.getMethod("asGeoJson");
			String outJson = (String)(outMethod.invoke(ogcGeometry));
			return new Text(outJson);
		} catch (Exception e1) {
			LogUtils.Log_InternalError(LOG, "ST_AsGeoJson: " + e1);
			Geometry esriGeom = ogcGeometry.getEsriGeometry();
			// SpatialReference spatialReference = null;
			// if (wkid != GeometryUtils.WKID_UNKNOWN) {
			// 	spatialReference = SpatialReference.create(wkid);
			// }
			int wkid = GeometryUtils.getWKID(geomref);
			try {
				Method outMethod = GeometryEngine.class.getMethod("geometryToGeoJSON");
				String outJson = (String)(outMethod.invoke(wkid, esriGeom));
				return new Text(outJson);
			} catch (Exception e2) {
				LogUtils.Log_InternalError(LOG, "ST_AsGeoJson: " + e2);
				try {
					Method outMethod = GeometryEngine.class.getMethod("geometryToGeoJson");
					String outJson = (String)(outMethod.invoke(wkid, esriGeom));
					return new Text(outJson);
				} catch (Exception e3) {
					LogUtils.Log_InternalError(LOG, "ST_AsGeoJson: " + e3);
					return null;
				}
			}
		}
	}

}
