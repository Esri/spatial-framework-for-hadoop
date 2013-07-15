package com.esri.hadoop.hive;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.hadoop.hive.GeometryUtils.OGCType;

@Description(
		name = "ST_Aggr_Union",
		value = "_FUNC_(st_geometry) - aggregate union of all geometries passed",
		extended = "Example:\n"
			+ "  SELECT _FUNC_(geometry) FROM source; -- return union of all geometries in source"
		)
public class ST_Aggr_Union extends UDAF {

	public static class AggrUnionBinaryEvaluator implements UDAFEvaluator {
		
		static final Log LOG = LogFactory.getLog(ST_Aggr_Union.class.getName());
		
		int MAX_BUFFER_SIZE = 1000;
		
		private ArrayList<Geometry> geometries = new ArrayList<Geometry>(MAX_BUFFER_SIZE);
		
		/*
		 * Initialize evaluator
		 */
		@Override
		public void init(){
			
			if (geometries.size() > 0){
				geometries.clear();
			}
		}
		
		/*
		 * Iterate is called once per row in a table
		 */
		public boolean iterate(BytesWritable geomref) throws HiveException{

			if (geomref == null){
				return false;
			}

			addGeometryToBuffer(geomref);
			
			if (geometries.size() == 0){
				return false;
			}
			
			return true;
		}
		
		/*
		 * Return a geometry that is the union of all geometries added up until this point
		 */
		public BytesWritable terminatePartial() throws HiveException{

			maybeUnionBuffer(true);
			
			if (geometries.size() == 1){
				return GeometryUtils.geometryToEsriShapeBytesWritable(geometries.get(0), GeometryUtils.WKID_UNKNOWN, OGCType.ST_POLYGON);
			} else {
				return null;
			}
		}
		
		/*
		 * Return a geometry that is the union of all geometries added up until this point
		 */
		public BytesWritable terminate() throws HiveException{
			// for our purposes, terminate is the same as terminatePartial
			return terminatePartial();
		}
		
		/*
		 * Merge the current state of this evaluator with the result of another evaluator's terminatePartial()
		 */
		public boolean merge(BytesWritable other) throws HiveException {
			if (other == null){
				return false;
			}
			addGeometryToBuffer(other);
			return true;
		}
		
		private void addGeometryToBuffer(BytesWritable geomref) throws HiveException{
			OGCGeometry ogcGeometry = GeometryUtils.geometryFromEsriShape(geomref);
			addGeometryToBuffer(ogcGeometry.getEsriGeometry());
		}
		
		private void addGeometryToBuffer(Geometry geom) throws HiveException{
			geometries.add(geom);
			maybeUnionBuffer(false);
		}
		
		/*
		 * If the right conditions are met (or force == true), create a union of the geometries
		 * in the current buffer
		 */
		private void maybeUnionBuffer(boolean force) throws HiveException{
			
			if (geometries.size() < 2){
				return; // can't union a single geometry
			}
			
			if (force || geometries.size() > MAX_BUFFER_SIZE){
				Geometry [] geomArray = new Geometry[geometries.size()];
				geometries.toArray(geomArray);
				geometries.clear();
				
				try {
					LOG.error("performing union");
					Geometry unioned = GeometryEngine.union(geomArray, null);
					geometries.add(unioned);
				} catch (Exception e){
					LOG.error("exception thrown", e);
				}
			}
		}
	}
}