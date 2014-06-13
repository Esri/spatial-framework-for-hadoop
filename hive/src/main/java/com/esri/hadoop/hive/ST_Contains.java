package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.log4j.Logger;

import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.Geometry.GeometryAccelerationDegree;
import com.esri.core.geometry.ogc.OGCGeometry;

@UDFType(deterministic = true)
@Description(
		name = "ST_Contains",
		value = "_FUNC_(geometry1, geometry2) - return true if geometry1 contains geometry2",
		extended = "Example:\n" + 
		"SELECT _FUNC_(st_polygon(1,1, 1,4, 4,4, 4,1), st_point(2, 3) from src LIMIT 1;  -- return true\n" + 
		"SELECT _FUNC_(st_polygon(1,1, 1,4, 4,4, 4,1), st_point(8, 8) from src LIMIT 1;  -- return false"	
		)
public class ST_Contains extends GenericUDF {

	private static Logger LOG = Logger.getLogger(ST_ContainsGeneric.class);
	
	private static final int NUM_ARGS = 2;
	private static final int GEOM_1 = 0;
	private static final int GEOM_2 = 1;
	
	private transient HiveGeometryOIHelper geomHelper1;
	private transient HiveGeometryOIHelper geomHelper2;
	
	private transient OperatorContains opContains = OperatorContains.local();
	private transient boolean firstRun = true;
	
	public ST_Contains() {
		
	}
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] OIs)
			throws UDFArgumentException {

		if (OIs.length != NUM_ARGS) {
			throw new UDFArgumentException("The function ST_Contains requires 2 arguments.");
		}
		
		geomHelper1 = HiveGeometryOIHelper.create(OIs[GEOM_1], GEOM_1);
		geomHelper2 = HiveGeometryOIHelper.create(OIs[GEOM_2], GEOM_2);
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("OI[0]=" + geomHelper1);
			LOG.debug("OI[1]=" + geomHelper2);
		}

		firstRun = true;
		
		return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
	}
	
	@Override
	public Object evaluate(DeferredObject[] args) throws HiveException {
		
		OGCGeometry geom1 = geomHelper1.getGeometry(args);
		OGCGeometry geom2 = geomHelper2.getGeometry(args);
		
		if (geom1 == null || geom2 == null) {
			return false;
		}
		
		if (firstRun) {
			if (geomHelper1.isConstant()) {
				opContains.accelerateGeometry(geom1.getEsriGeometry(), 
						geom1.getEsriSpatialReference(), GeometryAccelerationDegree.enumMedium);
				LOG.info("Accelerating geometry1");
			}

			firstRun = false;
		}
		
		return opContains.execute(geom1.getEsriGeometry(), geom2.getEsriGeometry(), geom1.getEsriSpatialReference(), null);
	}

	public void close() {
		OperatorContains.deaccelerateGeometry(geomHelper1.getConstantGeometry().getEsriGeometry());
	}
	
	@Override
	public String getDisplayString(String[] args) {
		return String.format("returns true if %s contains %s", args[0], args[1]);
	}
}

