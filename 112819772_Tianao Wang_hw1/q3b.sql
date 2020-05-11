select deaData.buyerZip as zip, 
deaData.mme/(select distinct(cse532.zippop.ZPOP) 
from cse532.zippop where deaData.buyerZip = cse532.zippop.zip and cse532.zippop.ZPOP!=0) as MMENORBYPOP,
rank() over (order by deaData.mme/(select distinct(cse532.zippop.ZPOP) 
from cse532.zippop where deaData.buyerZip = cse532.zippop.zip and cse532.zippop.ZPOP!=0) desc) as RN

from(select cse532.dea_ny.buyer_zip as buyerZip,sum(cse532.dea_ny.mme) as mme from cse532.dea_ny 
inner join cse532.zippop on cse532.dea_ny.buyer_zip = cse532.zippop.zip 
group by cse532.dea_ny.buyer_zip) as deaData where (select distinct(cse532.zippop.ZPOP) 
from cse532.zippop where deaData.buyerZip = cse532.zippop.zip and cse532.zippop.ZPOP!=0) is not null limit 5;