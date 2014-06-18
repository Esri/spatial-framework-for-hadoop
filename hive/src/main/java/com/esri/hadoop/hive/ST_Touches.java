package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;

import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.OperatorTouches;

@Description(
	name = "ST_Touches",
	value = "_FUNC_(geometry1, geometry2) - return true if geometry1 touches geometry2",
	extended = "Example:\n" + 
	"SELECT _FUNC_(st_point(1, 2), st_polygon(1, 1, 1, 4, 4, 4, 4, 1)) from src LIMIT 1;  -- return true\n" + 
	"SELECT _FUNC_(st_point(8, 8), st_polygon(1, 1, 1, 4, 4, 4, 4, 1)) from src LIMIT 1;  -- return false"	
	)

public class ST_Touches extends ST_GeometryRelational {

	@Override
	protected OperatorSimpleRelation getRelationOperator() {
		return OperatorTouches.local();
	}

	@Override
	public String getDisplayString(String[] args) {
		return String.format("returns true if %s touches %s", args[0], args[1]);
	}
}
