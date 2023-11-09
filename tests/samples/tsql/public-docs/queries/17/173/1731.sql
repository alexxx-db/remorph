-- see https://learn.microsoft.com/en-us/sql/t-sql/spatial-geometry/stgeomcollfromwkb-geometry-data-type?view=sql-server-ver16

DECLARE @g geometry;  
SET @g = geometry::STGeomCollFromWKB(0x0107000000020000000103000000010000000400000000000000000014400000000000001440000000000000244000000000000014400000000000002440000000000000244000000000000014400000000000001440010100000000000000000024400000000000002440, 0);  
SELECT @g.STAsText();