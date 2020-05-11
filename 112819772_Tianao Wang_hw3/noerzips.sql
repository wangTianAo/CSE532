SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1;
select distinct left(ZipCode,5) as zip from cse532.facility where left(ZipCode,5) not in(
select zipA.zcta5ce10 from cse532.uszip zipA,cse532.uszip zipB 
where db2gse.ST_Intersects(zipA.shape,zipB.shape)=1 and
zipB.zcta5ce10 in (select left(ZipCode,5) from cse532.facility where FacilityID in (select FacilityID from cse532.facilitycertification where AttributeValue = 'Emergency Department')))
and left(ZipCode,5) not in (select left(ZipCode,5) from cse532.facility where FacilityID in (select FacilityID from cse532.facilitycertification where AttributeValue = 'Emergency Department'))
and left(ZipCode,5) in (select zcta5ce10 from cse532.uszip)
and left(ZipCode,1)<='9';
SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1;