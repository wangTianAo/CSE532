drop table cse532.facilitycertification;

create table cse532.facilitycertification(
	FacilityID VARCHAR(16),
	FacilityName VARCHAR(128),
	Description VARCHAR(128),
	AttributeType VARCHAR(128),
	AttributeValue VARCHAR(128),
	MeasureValue VARCHAR(128),
	County VARCHAR(16)
);

load from "D:\stonybrook\homework\database\hm3\Tianao Wang\Health_Facility_Certification_Information.csv" of del MESSAGES load.msg INSERT INTO cse532.facilitycertification;