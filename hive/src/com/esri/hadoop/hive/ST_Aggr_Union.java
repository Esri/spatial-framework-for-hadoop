package com.esri.hadoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.ListeningGeometryCursor;
import com.esri.core.geometry.OperatorUnion;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;

@Description(
	name = "ST_Aggr_Union",
	value = "_FUNC_(ST_Geometry) - aggregate union of all geometries passed",
	extended = "Example:\n"
		+ "  SELECT _FUNC_(geometry) FROM source; -- return union of all geometries in source"
	)

public class ST_Aggr_Union extends UDAF {
	static final Log LOG = LogFactory.getLog(ST_Aggr_Union.class.getName());

	public static class AggrUnionBinaryEvaluator implements UDAFEvaluator {
		
		SpatialReference spatialRef = null;
		int firstWKID = -2;
		ListeningGeometryCursor lgc = null;  // listening geometry cursor
		GeometryCursor xgc = null;           // executing geometry cursor
		
		/*
		 * Initialize evaluator
		 */
		@Override
		public void init() {  // no-op
		}
		
		/*
		 * Iterate is called once per row in a table
		 */
		public boolean iterate(BytesWritable geomref) throws HiveException {

			if (geomref == null) {
				LogUtils.Log_ArgumentsNull(LOG);
				return false;
			}

			if (xgc == null) {
				firstWKID = GeometryUtils.getWKID(geomref);
				if (firstWKID != GeometryUtils.WKID_UNKNOWN) {
					spatialRef = SpatialReference.create(firstWKID);
				}
				// Need new geometry cursors both initially and after every terminatePartial(),
				// because the geometry cursors can not be re-used after extracting the
				// unioned geometry with GeometryCursor.next().
				//Create an empty listener.
				lgc = new ListeningGeometryCursor();
				//Obtain union operator - after taking note of spatial reference.
				xgc = OperatorUnion.local().execute(lgc, spatialRef, null);
			} else if (firstWKID != GeometryUtils.getWKID(geomref)) {
				LogUtils.Log_SRIDMismatch(LOG, geomref, firstWKID);
				return false;
			}

			try {
				lgc.tick(GeometryUtils.geometryFromEsriShape(geomref).getEsriGeometry());   // push
				xgc.tock();   // tock to match tick
				return true;
			} catch (Exception e) {
				LogUtils.Log_InternalError(LOG, "ST_Aggr_Union: " + e);
				return false;
			}

		}

		/*
		 * Merge the current state of this evaluator with the result of another evaluator's terminatePartial()
		 */
		public boolean merge(BytesWritable other) throws HiveException {
			// for our purposes, merge is the same as iterate
			return iterate(other);
		}

		/*
		 * Return a geometry that is the union of all geometries added up until this point
		 */
		public BytesWritable terminatePartial() throws HiveException {
			try {
				Geometry rslt = xgc.next();
				lgc = null;  // not reusable
				xgc = null;  // not reusable
				OGCGeometry ogeom = OGCGeometry.createFromEsriGeometry(rslt, spatialRef);
				return GeometryUtils.geometryToEsriShapeBytesWritable(ogeom);
			} catch (Exception e) {
				LogUtils.Log_InternalError(LOG, "ST_Aggr_Union: " + e);
			}
			return null;
		}

		/*
		 * Return a geometry that is the union of all geometries added up until this point
		 */
		public BytesWritable terminate() throws HiveException{
			// for our purposes, terminate is the same as terminatePartial
			return terminatePartial();
		}

	}
}
