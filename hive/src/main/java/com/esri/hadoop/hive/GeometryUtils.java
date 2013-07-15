package com.esri.hadoop.hive;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;


import com.esri.core.geometry.*;
import com.esri.core.geometry.ogc.*;
public class GeometryUtils {
	
	private static final int SIZE_WKID = 4;
	private static final int SIZE_TYPE = 1;
	
	public static final int WKID_UNKNOWN = 0;
	
	public enum OGCType {
		UNKNOWN(0),
		ST_POINT(1),
		ST_LINESTRING(2),
		ST_POLYGON(3),
		ST_MULTIPOINT(4),
		ST_MULTILINESTRING(5),
		ST_MULTIPOLYGON(6);
		
		private final int index;
		
		OGCType(int index){
			this.index = index;
		}
		
		public int getIndex(){
			return this.index;
		}
	}
	
	public static OGCType [] OGCTypeLookup = {
		OGCType.UNKNOWN,
		OGCType.ST_POINT,
		OGCType.ST_LINESTRING,
		OGCType.ST_POLYGON,
		OGCType.ST_MULTIPOINT,
		OGCType.ST_MULTILINESTRING,
		OGCType.ST_MULTIPOLYGON
	};
	
	public static final WritableBinaryObjectInspector geometryTransportObjectInspector = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;

	/**
	 * 
	 * @param geomref1
	 * @param geomref2
	 * @return return true if both geometries are in the same spatial reference
	 */
	public static boolean compareSpatialReferences(BytesWritable geomref1, BytesWritable geomref2){
		return getWKID(geomref1) == getWKID(geomref2);
	}
	
	public static BytesWritable geometryToEsriShapeBytesWritable(MapGeometry mapGeometry) {
		return serialize(mapGeometry);
	}
	
	public static BytesWritable geometryToEsriShapeBytesWritable(Geometry geometry, int wkid, OGCType type) {
		return serialize(geometry, wkid, type);
	}

	public static BytesWritable geometryToEsriShapeBytesWritable(OGCGeometry geometry) {
		return serialize(geometry);
	}

	public static OGCGeometry geometryFromEsriShape(BytesWritable geomref) {
		ByteBuffer bbuf = ByteBuffer.allocate(4);
		bbuf.order(ByteOrder.LITTLE_ENDIAN);
	
		int wkid = getWKID(geomref);
		byte [] shapeBytes = getShapeBytes(geomref);
		
		//minimum for a shape, even an empty one, is the 4 byte type record
		if (shapeBytes.length < 4) {
			return null;
		} else {
			bbuf.put(shapeBytes, 0, 4);
			if (bbuf.getInt(0) == Geometry.Type.Unknown.value()) { //empty Geometry, intentional
				return null;
			} else {
				SpatialReference spatialReference = null;
				if (wkid != GeometryUtils.WKID_UNKNOWN){
					spatialReference = SpatialReference.create(wkid);
				}
				Geometry esriGeom = GeometryEngine.geometryFromEsriShape(shapeBytes, Geometry.Type.Unknown);
				return OGCGeometry.createFromEsriGeometry(esriGeom, spatialReference);
			}
		}
	}
	
	/**
	 * Gets the geometry type for the given hive geometry bytes
	 * 
	 * @param geomref reference to hive geometry bytes
	 * @return OGCType set in the 5th byte of the hive geometry bytes
	 */
	public static OGCType getType(BytesWritable geomref){
		// SIZE_WKID is the offset to the byte that stores the type information
		return OGCTypeLookup[(int)geomref.getBytes()[SIZE_WKID]];
	}
	
	/**
	 * Sets the geometry type (in place) for the given hive geometry bytes
	 * @param geomref reference to hive geometry bytes
	 * @param type OGC geometry type
	 */
	public static void setType(BytesWritable geomref, OGCType type){
		geomref.getBytes()[SIZE_WKID] = (byte) type.getIndex();
	}
	
	/**
	 * Gets the WKID for the given hive geometry bytes
	 * 
	 * @param geomref reference to hive geometry bytes
	 * @return WKID set in the first 4 bytes of the hive geometry bytes
	 */
	public static int getWKID(BytesWritable geomref){
		ByteBuffer bb = ByteBuffer.wrap(geomref.getBytes());
		return bb.getInt(0);
	}
	
	/**
	 * Sets the WKID (in place) for the given hive geometry bytes
	 * 
	 * @param geomref reference to hive geometry bytes
	 * @param wkid
	 */
	public static void setWKID(BytesWritable geomref, int wkid){
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(wkid);
	    System.arraycopy(bb.array(), 0, geomref.getBytes(), 0, SIZE_WKID);
	}
	
	public static OGCType getInferredOGCType(Geometry geom){
		switch (geom.getType()){
		case Polygon:
			return OGCType.ST_MULTIPOLYGON;
		case Polyline:
			return OGCType.ST_MULTILINESTRING;
		case MultiPoint:
			return OGCType.ST_MULTIPOINT;
		case Point:
			return OGCType.ST_POINT;
		}
		
		return OGCType.UNKNOWN;
	}
	
	private static byte[] getShapeBytes(BytesWritable geomref){
		byte [] geometryBytes = geomref.getBytes();
		
		byte [] shapeBytes = new byte[geometryBytes.length - SIZE_WKID - SIZE_TYPE];
		
		System.arraycopy(geometryBytes, SIZE_WKID + SIZE_TYPE, shapeBytes, 0, shapeBytes.length);
		
		return shapeBytes;
	}
	
	private static BytesWritable serialize(MapGeometry mapGeometry){
		int wkid = 0;
		
		SpatialReference spatialRef = mapGeometry.getSpatialReference();
		
		if (spatialRef != null){
			wkid = spatialRef.getID();
		}
		
		Geometry.Type esriType = mapGeometry.getGeometry().getType();
		OGCType ogcType;
		
		switch (esriType){
		case Point:
			ogcType = OGCType.ST_POINT;
			break;
		case Polyline:
			ogcType = OGCType.ST_LINESTRING;
			break;
		case Polygon:
			ogcType = OGCType.ST_POLYGON;
			break;
		default:
			ogcType = OGCType.UNKNOWN;
		}
		
		return serialize(mapGeometry.getGeometry(), wkid, ogcType);
	}

	private static BytesWritable serialize(OGCGeometry ogcGeometry) {
		int wkid;
		try {
			wkid = ogcGeometry.SRID();
		} catch (NullPointerException npe) {
			wkid = 0;
		}

		OGCType ogcType;
		String typeName;
		try {
			typeName = ogcGeometry.geometryType();

			if (typeName.equals("Point"))
				ogcType = OGCType.ST_POINT;
			else if (typeName.equals("LineString"))
				ogcType = OGCType.ST_LINESTRING;
			else if (typeName.equals("Polygon"))
				ogcType = OGCType.ST_POLYGON;
			else if (typeName.equals("MultiPoint"))
				ogcType = OGCType.ST_MULTIPOINT;
			else if (typeName.equals("MultiLineString"))
				ogcType = OGCType.ST_MULTILINESTRING;
			else if (typeName.equals("MultiPolygon"))
				ogcType = OGCType.ST_MULTIPOLYGON;
			else
				ogcType = OGCType.UNKNOWN;
		} catch (NullPointerException npe) {
			ogcType = OGCType.UNKNOWN;
		}

		return serialize(ogcGeometry.getEsriGeometry(), wkid, ogcType);
	}

	private static BytesWritable serialize(Geometry geometry, int wkid, OGCType type){
		if (geometry == null) {
			return null;
		}
		
		// first get shape buffer for geometry
		byte[] shape = GeometryEngine.geometryToEsriShape(geometry);

		if (shape == null) {
			return null;
		}
		
		byte[] shapeWithData = new byte[shape.length + SIZE_WKID + SIZE_TYPE];
		
		System.arraycopy(shape, 0, shapeWithData, SIZE_WKID + SIZE_TYPE, shape.length);
		
		BytesWritable hiveGeometryBytes = new BytesWritable(shapeWithData);
		
		setWKID(hiveGeometryBytes, wkid);
		setType(hiveGeometryBytes, type);
		
		return new BytesWritable(shapeWithData);
	}
	
	
}
