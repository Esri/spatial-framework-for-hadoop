create temporary function ST_AsBinary as 'com.esri.hadoop.hive.ST_AsBinary';
create temporary function ST_AsGeoJSON as 'com.esri.hadoop.hive.ST_AsGeoJson';
create temporary function ST_AsJSON as 'com.esri.hadoop.hive.ST_AsJson';
create temporary function ST_AsShape as 'com.esri.hadoop.hive.ST_AsShape';
create temporary function ST_AsText as 'com.esri.hadoop.hive.ST_AsText';
create temporary function ST_GeomFromJSON as 'com.esri.hadoop.hive.ST_GeomFromJson';
create temporary function ST_GeomFromGeoJSON as 'com.esri.hadoop.hive.ST_GeomFromGeoJson';
create temporary function ST_GeomFromShape as 'com.esri.hadoop.hive.ST_GeomFromShape';
create temporary function ST_GeomFromText as 'com.esri.hadoop.hive.ST_GeomFromText';
create temporary function ST_GeomFromWKB as 'com.esri.hadoop.hive.ST_GeomFromWKB';
create temporary function ST_PointFromWKB as 'com.esri.hadoop.hive.ST_PointFromWKB';
create temporary function ST_LineFromWKB as 'com.esri.hadoop.hive.ST_LineFromWKB';
create temporary function ST_PolyFromWKB as 'com.esri.hadoop.hive.ST_PolyFromWKB';
create temporary function ST_MPointFromWKB as 'com.esri.hadoop.hive.ST_MPointFromWKB';
create temporary function ST_MLineFromWKB as 'com.esri.hadoop.hive.ST_MLineFromWKB';
create temporary function ST_MPolyFromWKB as 'com.esri.hadoop.hive.ST_MPolyFromWKB';
create temporary function ST_GeomCollection as 'com.esri.hadoop.hive.ST_GeomCollection';

create temporary function ST_GeometryType as 'com.esri.hadoop.hive.ST_GeometryType';

create temporary function ST_Point as 'com.esri.hadoop.hive.ST_Point';
create temporary function ST_PointZ as 'com.esri.hadoop.hive.ST_PointZ';
create temporary function ST_LineString as 'com.esri.hadoop.hive.ST_LineString';
create temporary function ST_Polygon as 'com.esri.hadoop.hive.ST_Polygon';

create temporary function ST_MultiPoint as 'com.esri.hadoop.hive.ST_MultiPoint';
create temporary function ST_MultiLineString as 'com.esri.hadoop.hive.ST_MultiLineString';
create temporary function ST_MultiPolygon as 'com.esri.hadoop.hive.ST_MultiPolygon';

create temporary function ST_SetSRID as 'com.esri.hadoop.hive.ST_SetSRID';

create temporary function ST_SRID as 'com.esri.hadoop.hive.ST_SRID';
create temporary function ST_IsEmpty as 'com.esri.hadoop.hive.ST_IsEmpty';
create temporary function ST_IsSimple as 'com.esri.hadoop.hive.ST_IsSimple';
create temporary function ST_Dimension as 'com.esri.hadoop.hive.ST_Dimension';
create temporary function ST_X as 'com.esri.hadoop.hive.ST_X';
create temporary function ST_Y as 'com.esri.hadoop.hive.ST_Y';
create temporary function ST_MinX as 'com.esri.hadoop.hive.ST_MinX';
create temporary function ST_MaxX as 'com.esri.hadoop.hive.ST_MaxX';
create temporary function ST_MinY as 'com.esri.hadoop.hive.ST_MinY';
create temporary function ST_MaxY as 'com.esri.hadoop.hive.ST_MaxY';
create temporary function ST_IsClosed as 'com.esri.hadoop.hive.ST_IsClosed';
create temporary function ST_IsRing as 'com.esri.hadoop.hive.ST_IsRing';
create temporary function ST_Length as 'com.esri.hadoop.hive.ST_Length';
create temporary function ST_GeodesicLengthWGS84 as 'com.esri.hadoop.hive.ST_GeodesicLengthWGS84';
create temporary function ST_Area as 'com.esri.hadoop.hive.ST_Area';
create temporary function ST_Is3D as 'com.esri.hadoop.hive.ST_Is3D';
create temporary function ST_Z as 'com.esri.hadoop.hive.ST_Z';
create temporary function ST_MinZ as 'com.esri.hadoop.hive.ST_MinZ';
create temporary function ST_MaxZ as 'com.esri.hadoop.hive.ST_MaxZ';
create temporary function ST_IsMeasured as 'com.esri.hadoop.hive.ST_IsMeasured';
create temporary function ST_M as 'com.esri.hadoop.hive.ST_M';
create temporary function ST_MinM as 'com.esri.hadoop.hive.ST_MinM';
create temporary function ST_MaxM as 'com.esri.hadoop.hive.ST_MaxM';
create temporary function ST_CoordDim as 'com.esri.hadoop.hive.ST_CoordDim';
create temporary function ST_NumPoints as 'com.esri.hadoop.hive.ST_NumPoints';
create temporary function ST_PointN as 'com.esri.hadoop.hive.ST_PointN';
create temporary function ST_StartPoint as 'com.esri.hadoop.hive.ST_StartPoint';
create temporary function ST_EndPoint as 'com.esri.hadoop.hive.ST_EndPoint';
create temporary function ST_ExteriorRing as 'com.esri.hadoop.hive.ST_ExteriorRing';
create temporary function ST_NumInteriorRing as 'com.esri.hadoop.hive.ST_NumInteriorRing';
create temporary function ST_InteriorRingN as 'com.esri.hadoop.hive.ST_InteriorRingN';
create temporary function ST_NumGeometries as 'com.esri.hadoop.hive.ST_NumGeometries';
create temporary function ST_GeometryN as 'com.esri.hadoop.hive.ST_GeometryN';
create temporary function ST_Centroid as 'com.esri.hadoop.hive.ST_Centroid';

create temporary function ST_Contains as 'com.esri.hadoop.hive.ST_Contains';
create temporary function ST_Crosses as 'com.esri.hadoop.hive.ST_Crosses';
create temporary function ST_Disjoint as 'com.esri.hadoop.hive.ST_Disjoint';
create temporary function ST_EnvIntersects as 'com.esri.hadoop.hive.ST_EnvIntersects';
create temporary function ST_Envelope as 'com.esri.hadoop.hive.ST_Envelope';
create temporary function ST_Equals as 'com.esri.hadoop.hive.ST_Equals';
create temporary function ST_Overlaps as 'com.esri.hadoop.hive.ST_Overlaps';
create temporary function ST_Intersects as 'com.esri.hadoop.hive.ST_Intersects';
create temporary function ST_Relate as 'com.esri.hadoop.hive.ST_Relate';
create temporary function ST_Touches as 'com.esri.hadoop.hive.ST_Touches';
create temporary function ST_Within as 'com.esri.hadoop.hive.ST_Within';

create temporary function ST_Distance as 'com.esri.hadoop.hive.ST_Distance';
create temporary function ST_Boundary as 'com.esri.hadoop.hive.ST_Boundary';
create temporary function ST_Buffer as 'com.esri.hadoop.hive.ST_Buffer';
create temporary function ST_ConvexHull as 'com.esri.hadoop.hive.ST_ConvexHull';
create temporary function ST_Intersection as 'com.esri.hadoop.hive.ST_Intersection';
create temporary function ST_Union as 'com.esri.hadoop.hive.ST_Union';
create temporary function ST_Difference as 'com.esri.hadoop.hive.ST_Difference';
create temporary function ST_SymmetricDiff as 'com.esri.hadoop.hive.ST_SymmetricDiff';
create temporary function ST_SymDifference as 'com.esri.hadoop.hive.ST_SymmetricDiff';

create temporary function ST_Aggr_ConvexHull as 'com.esri.hadoop.hive.ST_Aggr_ConvexHull';
create temporary function ST_Aggr_Intersection as 'com.esri.hadoop.hive.ST_Aggr_Intersection';
create temporary function ST_Aggr_Union as 'com.esri.hadoop.hive.ST_Aggr_Union';

create temporary function ST_Bin as 'com.esri.hadoop.hive.ST_Bin';
create temporary function ST_BinEnvelope as 'com.esri.hadoop.hive.ST_BinEnvelope';
