select  p.regimeid , reg.regimeesquema,count(*) 
from prescription p left join regimeterapeutico reg
on p.regimeid= reg.regimeid

where reg.active =FALSE

group by  p.regimeid , reg.regimeesquema  order by  reg.regimeesquema  asc 



update prescription set regimeid = 707806
where regimeid = 675528;


-- "ABC60+3TC30+LPV/r40/10"
update prescription set regimeid = 624397
where regimeid = 1523673;

-- "ABC+AZT+3TC+LPV/r"
update prescription set regimeid = 1523682
where regimeid = 3164894;

update prescription set regimeid = 1523682
where regimeid = 1523682;

-- "ATV/R 300/100"
update prescription set regimeid = 1380650
where regimeid = 1372635;

-- "AZT+3TC+ATV/r"
update prescription set regimeid = 1380650
where regimeid = 1380764;


-- "AZT60+3TC30"
update prescription set regimeid = 623998
where regimeid = 789588;


-- "AZT60+3TC+30ABC60"
update prescription set regimeid = 394311
where regimeid = 41119;

--"TDF+3TC"
update prescription set regimeid = 41095
where regimeid = 494183 ;

-- "TDF+3TC+ATV/r+RAL"
update prescription set regimeid = 1372631
where regimeid = 1615302 ;

-- "TDF+3TC+ATV/r+RAL"
update prescription set regimeid = 1372631
where regimeid = 1523683 ;



-- "TDF+3TC+DTG (2ª Linha)"

update prescription set regimeid = 1523738
where regimeid = 232378 ;

-- "TDF+3TC+RAL+DRV/r"
update prescription set regimeid = 624458
where regimeid in ( 675527 ,1615297 , 1615308);



-- "TDF+AZT+3TC+LPV/r"


update prescription set regimeid = 624583
where regimeid = 1615300 ;


















