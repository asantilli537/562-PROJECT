with getmax as (
	select cust, max(quant) from sales
	group by cust
),

firsthalf as (
	select sales.cust, count(sales.quant) from sales, getmax
	where sales.quant > (getmax.max / 2) and sales.month <= 6
	and sales.cust = getmax.cust
	group by sales.cust
),

secondhalf as (
	select sales.cust, count(sales.quant) from sales, getmax
	where sales.quant > (getmax.max / 2) and sales.month >= 7
	and sales.cust = getmax.cust
	group by sales.cust
)

select firsthalf.cust, firsthalf.count, secondhalf.count
from firsthalf, secondhalf
where firsthalf.cust = secondhalf.cust
order by firsthalf.cust