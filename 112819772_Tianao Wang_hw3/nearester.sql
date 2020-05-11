SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1;
select FacilityName,Geolocation,db2gse.st_distance(Geolocation,db2gse.st_point(40.824369, -72.993983, 1),'STATUTE MILE') as distance 
from cse532.facility where db2gse.ST_WITHIN(Geolocation,db2gse.st_buffer(db2gse.st_point(40.824369, -72.993983, 1),10,'STATUTE MILE'))=1  
and FacilityID in (select FacilityID from cse532.facilitycertification where AttributeValue = 'Emergency Department')
order by distance LIMIT 1;
SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1;