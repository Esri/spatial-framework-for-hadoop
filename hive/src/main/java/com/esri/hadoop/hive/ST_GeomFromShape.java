package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.io.BytesWritable;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.SpatialReference;
import com.esri.hadoop.hive.GeometryUtils.OGCType;

public class ST_GeomFromShape extends ST_Geometry {

	static final Log LOG = LogFactory.getLog(ST_GeomFromShape.class.getName());

	public BytesWritable evaluate(BytesWritable shape) throws UDFArgumentException {
		return evaluate(shape, 0);
	}

	public BytesWritable evaluate(BytesWritable shape, int wkid) throws UDFArgumentException  {

		// String wkt = shape.toString();
		LOG.error("geom-from-shape"); /// debug/todo
		try {
			SpatialReference spatialReference = null;
			if (wkid != GeometryUtils.WKID_UNKNOWN) {
				spatialReference = SpatialReference.create(wkid);
			}
			// byte [] byteArr = shape.getBytes();
			// ByteBuffer byteBuf = ByteBuffer.allocate(byteArr.length);
			// byteBuf.put(byteArr);
			// TODO: little-endian order: .order(ByteOrder.LITTLE_ENDIAN)
			//OGCGeometry ogcObj = OGCGeometry.fromEsriShape(ByteBuffer.wrap(shape.getBytes()));
			//ogcObj.setSpatialReference(spatialReference);
			
			Geometry geometry = GeometryEngine.geometryFromEsriShape(shape.getBytes(), Geometry.Type.Unknown);
			switch (geometry.getType())
			{
			case Point:
				return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_POINT);
				
			case MultiPoint:
				return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_MULTIPOINT);
				
			case Line:
				return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_LINESTRING);
				
			case Polyline:
				return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_MULTILINESTRING);
				
			case Envelope:
				return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_POLYGON);
				
			case Polygon:
				return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.ST_MULTIPOLYGON);
				
			default:
				return GeometryUtils.geometryToEsriShapeBytesWritable(geometry, wkid, OGCType.UNKNOWN);
			}
		} catch (Exception e) {
			LogUtils.Log_ExceptionThrown(LOG, "geom-from-shape", e); // todo
			return null;
		}
	}

}