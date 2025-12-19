with ny as (
	select cust, sum(quant), avg(quant) from sales
	where sales.state = 'NY'
	group by cust
),

nj as (
	select cust, sum(quant), avg(quant) from sales
	where sales.state = 'NJ'
	group by cust
),

ct as (
	select cust, sum(quant), avg(quant) from sales
	where sales.state = 'CT'
	group by cust
)

select ny.cust, ny.sum as "NY_SUM", nj.sum AS "NJ_SUM", ct.sum as "CT_SUM"
from ny, nj, ct
where ny.cust = nj.cust and ct.cust = nj.cust and (ny.sum > ny.sum * 2 or ny.avg > ct.avg)
order by ny.cust