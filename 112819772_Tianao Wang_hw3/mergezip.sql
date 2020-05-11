DROP PROCEDURE mergezip;@

create procedure mergezip()
language sql
begin
	DECLARE SQLSTATE CHAR(5) DEFAULT '00000';
	DECLARE pavg double;
	DECLARE psum double;
	DECLARE pnumber double;
	DECLARE psal double;
	DECLARE c CURSOR FOR SELECT zpop FROM cse532.zippop where zpop>0;
	SET psum = 0;
	SET pnumber = 0;
	open c;
	fetch from c into psal;
	while(SQLSTATE='00000') DO
		set psum = psum+psal;
		set pnumber = pnumber+1;
		fetch from c into psal;
	end while;
	close c;
	set pavg = psum/pnumber;
	
	drop table if exists newZip;
	CREATE table newZip (zipCode Varchar(50), shape db2gse.ST_MULTIPOLYGON, zpop INTEGER);
	DECLARE oldzip1 Varchar(50);
	DECLARE oldzip2 Varchar(50);
	DECLARE pop1 double;
	DECLARE pop2 double;
	DECLARE c CURSOR FOR SELECT zcta5ce10 FROM cse532.uszip where zcta5ce10 in (select zip from cse532.zippop where zpop<pavg);
	open c;
	fetch from c into oldzip;
	while(SQLSTATE='00000') DO
		DECLARE d CURSOR FOR select zipB.zcta5ce10 from cse532.uszip zipA,cse532.uszip zipB where db2gse.ST_Intersects((select shape from uszip where zcta5ce10=oldzip),zipB.shape)=1;
		open d;
		fetch from d into oldzip2;
		while(SQLSTATE='00000') DO
			set pop1 = (select zpop from zippop where zip = oldzip1);
			set pop2 = (select zpop from zippop where zip = oldzip2);
			if pop1+pop2>pavg
			THEN insert into newZip values(oldzip1,db2gse.st_UNION((select shape from uszip where zcta5ce10=oldzip1),(select shape from uszip where zcta5ce10=oldzip2),0),oldzip1);
			end if;
		end while;
		close d;
	end while;
	close c;
end;@

call mergezip();@


