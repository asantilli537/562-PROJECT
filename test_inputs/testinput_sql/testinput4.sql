with getavg as (
	select cust, avg(quant) from sales
	group by cust
),

firsthalf as (
	select sales.cust, count(sales.quant) from sales, getavg
	where sales.quant > (getavg.avg) and sales.month <= 6
	and sales.cust = getavg.cust
	group by sales.cust
),

secondhalf as (
	select sales.cust, count(sales.quant) from sales, getavg
	where sales.quant > (getavg.avg) and sales.month >= 7
	and sales.cust = getavg.cust
	group by sales.cust
)

select firsthalf.cust, firsthalf.count, secondhalf.count
from firsthalf, secondhalf
where firsthalf.cust = secondhalf.cust
order by firsthalf.cust