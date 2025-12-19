with monthavg as (
	select prod, month, avg(quant) from sales
	group by prod, month
),
yearavg as (
	select prod, avg(quant) from sales
	group by prod
)
select monthavg.prod, monthavg.month, (monthavg.avg / yearavg.avg)
from yearavg, monthavg
where yearavg.prod = monthavg.prod

order by monthavg.prod, monthavg.month