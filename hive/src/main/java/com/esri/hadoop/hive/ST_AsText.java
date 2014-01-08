package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;


import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.WktExportFlags;
import com.esri.hadoop.hive.GeometryUtils.OGCType;

import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_AsText",
	value = "_FUNC_(ST_Geometry) - return Well-Known Text (WKT) representation of ST_Geometry\n",
	extended = "Example:\n" +
	"  SELECT _FUNC_(ST_Point(1, 2)) FROM onerow;  --  POINT (1 2)\n"
	)
//@HivePdkUnitTests(
//	cases = { 
//		@HivePdkUnitTest(
//			query = "SELECT ST_AsText(ST_Point(1, 2)), ST_AsText(ST_MultiPoint(1, 2, 3, 4)) FROM onerow",
//			result = "POINT (1 2)	MULTIPOINT ((1 2), (3 4))"
//			),
//		@HivePdkUnitTest(
//			query = "SELECT ST_AsText(ST_LineString(1, 1, 2, 2, 3, 3)) FROM onerow",
//			result = "LINESTRING (1 1, 2 2, 3 3)"
//			),
//		@HivePdkUnitTest(
//			query = "SELECT ST_AsText(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)), ST_AsText(ST_Polygon(1, 1, 4, 1, 4, 4, 1, 4)) FROM onerow",
//			result = "POLYGON ((4 1, 4 4, 1 4, 1 1, 4 1))	NULL"
//			),
//		@HivePdkUnitTest(
//			query = "SELECT ST_AsText(ST_MultiPolygon(array(1, 1, 1, 4, 4, 4, 4, 1), array(11, 11, 11, 14, 14, 14, 14, 11))) FROM onerow",
//			result = "MULTIPOLYGON (((4 1, 4 4, 1 4, 1 1, 4 1)), ((14 11, 14 14, 11 14, 11 11, 14 11)))"
//			)
//		}
//	)
public class ST_AsText extends ST_Geometry {
	
	static final Log LOG = LogFactory.getLog(ST_AsText.class.getName());
	
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
		
		int wktExportFlag = getWktExportFlag(GeometryUtils.getType(geomref));
		
		try {
			// mind: GeometryType with ST_AsText(ST_GeomFromText('MultiLineString((0 80, 0.03 80.04))'))
			// return new Text(ogcGeometry.asText());
			return new Text(GeometryEngine.geometryToWkt(ogcGeometry.getEsriGeometry(), wktExportFlag));
		} catch (Exception e){
			LOG.error(e.getMessage());
			return null;
		}
	}
	
	private int getWktExportFlag(OGCType type){
		switch (type){
		case ST_POLYGON:
			return WktExportFlags.wktExportPolygon;
		case ST_MULTIPOLYGON:
			return WktExportFlags.wktExportMultiPolygon;
		case ST_POINT:
			return WktExportFlags.wktExportPoint;
		case ST_MULTIPOINT:
			return WktExportFlags.wktExportMultiPoint;
		case ST_LINESTRING:
			return WktExportFlags.wktExportLineString;
		case ST_MULTILINESTRING:
			return WktExportFlags.wktExportMultiLineString;
		default:
			return WktExportFlags.wktExportDefaults;
		}
	}
}