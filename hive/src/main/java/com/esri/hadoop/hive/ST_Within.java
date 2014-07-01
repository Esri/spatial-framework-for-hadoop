package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;

import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.OperatorWithin;

@Description(
	name = "ST_Within",
	value = "_FUNC_(geometry1, geometry2) - return true if geometry1 is within geometry2",
	extended = "Example:\n" + 
	"SELECT _FUNC_(st_point(2, 3), st_polygon(1,1, 1,4, 4,4, 4,1)) from src LIMIT 1;  -- return true\n" + 
	"SELECT _FUNC_(st_point(8, 8), st_polygon(1,1, 1,4, 4,4, 4,1)) from src LIMIT 1;  -- return false"	
	)

public class ST_Within extends ST_GeometryRelational {

	@Override
	protected OperatorSimpleRelation getRelationOperator() {
		return OperatorWithin.local();
	}

	@Override
	public String getDisplayString(String[] args) {
		return String.format("returns true if %s within %s", args[0], args[1]);
	}
}
