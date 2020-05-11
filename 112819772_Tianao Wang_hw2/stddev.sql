DROP PROCEDURE stddev;@

create procedure stddev(out stddev double)
language sql
begin
	DECLARE SQLSTATE CHAR(5) DEFAULT '00000';
	DECLARE psum double;
	DECLARE pavg double;
	DECLARE pnumber double;
	DECLARE psal double;
	DECLARE ppowSum double;
	DECLARE c CURSOR FOR SELECT SALARY FROM EMPLOYEE;
	SET psum = 0;
	SET pnumber = 0;
	SET ppowSum = 0;
	open c;
	fetch from c into psal;
	while(SQLSTATE='00000') DO
		set psum = psum+psal;
		set pnumber = pnumber+1;
		fetch from c into psal;
	end while;
	close c;
	set pavg = psum/pnumber;
	open c;
	fetch from c into psal;
	while(SQLSTATE='00000') DO
		set ppowSum = ppowSum+pow(psal-pavg,2);
		fetch from c into psal;
	end while;
	close c;
	set stddev = sqrt(ppowSum/pnumber);
end;@

call stddev(?);@


