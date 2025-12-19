with ny as (
	select cust, max(quant) from sales
	where sales.state = 'NY' and sales.quant < 10
	group by cust
),

nj as (
	select cust, max(quant) from sales
	where sales.state = 'NJ' and sales.quant < 10
	group by cust
),

ct as (
	select cust, max(quant) from sales
	where sales.state = 'CT' and sales.quant < 10
	group by cust
)

select ny.cust, ny.max as "NY_MAX", nj.max AS "NJ_MAX", ct.max as "CT_MAX"
from ny, nj, ct
where ny.cust = nj.cust and ct.cust = nj.cust
order by ny.cust