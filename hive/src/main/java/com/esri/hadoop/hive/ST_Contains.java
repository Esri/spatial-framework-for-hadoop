package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.UDFType;

import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.OperatorSimpleRelation;

@UDFType(deterministic = true)
@Description(
		name = "ST_Contains",
		value = "_FUNC_(geometry1, geometry2) - return true if geometry1 contains geometry2",
		extended = "Example:\n" + 
		"SELECT _FUNC_(st_polygon(1,1, 1,4, 4,4, 4,1), st_point(2, 3) from src LIMIT 1;  -- return true\n" + 
		"SELECT _FUNC_(st_polygon(1,1, 1,4, 4,4, 4,1), st_point(8, 8) from src LIMIT 1;  -- return false"	
		)
public class ST_Contains extends ST_GeometryRelational {

	@Override
	protected OperatorSimpleRelation getRelationOperator() {
		return OperatorContains.local();
	}
	
	@Override
	public String getDisplayString(String[] args) {
		return String.format("returns true if %s contains %s", args[0], args[1]);
	}
}

