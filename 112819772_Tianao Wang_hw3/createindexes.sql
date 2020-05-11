drop index cse532.facilityidx;
drop index cse532.zipidx;
drop index cse532.zipzip;
drop index cse532.facilityzip;
drop index cse532.facilityid;
drop index cse532.facilitycertificationid;
drop index cse532.certificationAttributeValue;

create index cse532.facilityidx on cse532.facility(geolocation) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.zipidx on cse532.uszip(shape) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.zipzip on cse532.uszip(zcta5ce10);

create index cse532.facilityzip on cse532.facility(zipcode);

create index cse532.facilityid on cse532.facility(facilityid);

create index cse532.facilitycertificationid on cse532.facilitycertification(facilityid);

create index cse532.certificationAttributeValue on cse532.facilitycertification(AttributeValue);

runstats on table cse532.facility and indexes all;

runstats on table cse532.uszip and indexes all;