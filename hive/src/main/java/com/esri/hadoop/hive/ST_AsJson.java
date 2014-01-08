package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;


import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;

import com.esri.core.geometry.ogc.OGCGeometry;


@Description(
	name = "ST_AsJSON",
	value = "_FUNC_(ST_Geometry) - return JSON representation of ST_Geometry\n",
	extended = "Example:\n" +
	"  SELECT _FUNC_(ST_Point(1.0, 2.0)) from onerow; -- {\"x\":1.0,\"y\":2.0}\n" +
	"  SELECT _FUNC_(ST_SetSRID(ST_Point(1, 1), 4326)) from onerow; -- {\"x\":1.0,\"y\":1.0,\"spatialReference\":{\"wkid\":4326}}")
//@HivePdkUnitTests(
//	cases = { 
//		@HivePdkUnitTest(
//			query = "select ST_AsJSON(ST_Point(1, 2)), ST_AsJSON(ST_SetSRID(ST_Point(1, 1), 4326)) from onerow",
//			result = "{\"x\":1.0,\"y\":2.0}	{\"x\":1.0,\"y\":1.0,\"spatialReference\":{\"wkid\":4326}}"
//			),
//		@HivePdkUnitTest(
//			query = "SELECT ST_AsJSON(ST_MultiLineString(array(1, 1, 2, 2, 3, 3), array(10, 10, 11, 11, 12, 12))) from onerow",
//			result = "{\"paths\":[[[1.0,1.0],[2.0,2.0],[3.0,3.0]],[[10.0,10.0],[11.0,11.0],[12.0,12.0]]]}"
//			),
//		@HivePdkUnitTest(
//			query = "SELECT ST_AsJSON(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)), ST_AsJSON(ST_Polygon(1, 1)) from onerow",
//			result = "{\"rings\":[[[1.0,1.0],[1.0,4.0],[4.0,4.0],[4.0,1.0],[1.0,1.0]]]}	NULL"
//			)
//		}
//	)
public class ST_AsJson extends ST_Geometry {
	static final Log LOG = LogFactory.getLog(ST_AsJson.class.getName());
	
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

		Geometry esriGeom = ogcGeometry.getEsriGeometry();
		int wkid = GeometryUtils.getWKID(geomref);
		return new Text(GeometryEngine.geometryToJson(wkid, esriGeom));
	}
}
