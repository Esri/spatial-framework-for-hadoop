select ST_Area(ST_BinEnvelope(1.0, ST_Bin(1.0, ST_Point(0, 0)))) from onerow;
select ST_AsText(ST_BinEnvelope(1.0, ST_Bin(1.0, ST_Point(0, 0)))) from onerow;
