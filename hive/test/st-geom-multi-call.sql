select ST_Length(ST_Linestring(1,1, 1,2, 2,2, 2,1)), 
ST_Length(ST_Linestring(1,1, 1,4, 4,4, 4,1)),
ST_Length(ST_Linestring(1,1, 1,7, 7,7, 7,1)) from onerow;
select ST_Area(ST_Polygon(1,1, 1,2, 2,2, 2,1)),
ST_Area(ST_Polygon(1,1, 1,4, 4,4, 4,1)) from onerow;
select ST_Contains(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1), ST_Point(2, 3)),
ST_Contains(ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1), ST_Point(8, 8)) from onerow;
select ST_CoordDim(ST_Point(0., 3.)),
ST_CoordDim(ST_PointZ(0., 3., 1)) from onerow;
select ST_Crosses(st_linestring(2,0, 2,3), ST_Polygon(1,1, 1,4, 4,4, 4,1)),
ST_Crosses(st_linestring(8,7, 7,8), ST_Polygon(1,1, 1,4, 4,4, 4,1)) from onerow;
select ST_Dimension(ST_Point(0,0)),
ST_Dimension(ST_LineString(1.5,2.5, 3.0,2.2)) from onerow;
select ST_Disjoint(st_point(1,1), ST_Point(1,1)),
ST_Disjoint(st_point(2,0), ST_Point(1,1)) from onerow;
select ST_EnvIntersects(st_point(1,1), ST_Point(1,1)),
ST_EnvIntersects(st_point(2,0), ST_Point(1,1)) from onerow;
select ST_Equals(st_point(1,1), ST_Point(1,1)),
ST_Equals(st_point(2,0), ST_Point(1,1)) from onerow;
select ST_Intersects(st_point(1,1), ST_Point(1,1)),
ST_Intersects(st_point(2,0), ST_Point(1,1)) from onerow;
select ST_Is3D(ST_Point(0., 3.)),
ST_Is3D(ST_PointZ(0., 3., 1)) from onerow;
select ST_Overlaps(st_polygon(2,0, 2,3, 3,0), ST_Polygon(1,1, 1,4, 4,4, 4,1)),
ST_Overlaps(st_polygon(2,0, 2,1, 3,1), ST_Polygon(1,1, 1,4, 4,4, 4,1)) from onerow;
select ST_Touches(ST_Point(1, 3), ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)),
ST_Touches(ST_Point(8, 8), ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)) from onerow;
select ST_Within(ST_Point(2, 3), ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)),
ST_Within(ST_Point(8, 8), ST_Polygon(1, 1, 1, 4, 4, 4, 4, 1)) from onerow;
SELECT ST_Intersects(ST_GeomFromGeoJson('{"type": "LineString", "coordinates": [[2.5,2.5], [8.0,0.0]]}'),
                     ST_GeomFromGeoJson('{"type": "LineString", "coordinates": [[1.5,1.5], [0.0,7.0]]}'))  from onerow;
SELECT ST_Intersects(ST_GeomFromJson('{"paths":[[[2.5,2.5],[8,0]]],"spatialReference":{"wkid":4326}}'),
                     ST_GeomFromJson('{"paths":[[[1.5,1.5],[0,7]]],"spatialReference":{"wkid":4326}}')) from onerow;
