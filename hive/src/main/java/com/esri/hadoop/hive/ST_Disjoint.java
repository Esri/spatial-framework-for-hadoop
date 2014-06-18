package com.esri.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.Description;

import com.esri.core.geometry.OperatorDisjoint;
import com.esri.core.geometry.OperatorSimpleRelation;

@Description(
	name = "ST_Disjoint",
	value = "_FUNC_(ST_Geometry1, ST_Geometry2) - return true if ST_Geometry1 intersects ST_Geometry2",
	extended = "Example:\n" +
	"SELECT _FUNC_(ST_LineString(0,0, 0,1), ST_LineString(1,1, 1,0)) from src LIMIT 1;  -- return true\n" +
	"SELECT _FUNC_(ST_LineString(0,0, 1,1), ST_LineString(1,0, 0,1)) from src LIMIT 1;  -- return false\n"	
	)

public class ST_Disjoint extends ST_GeometryRelational {

	@Override
	protected OperatorSimpleRelation getRelationOperator() {
		return OperatorDisjoint.local();
	}

	@Override
	public String getDisplayString(String[] args) {
		return String.format("returns true if %s and %s are disjoint", args[0], args[1]);
	}
}
