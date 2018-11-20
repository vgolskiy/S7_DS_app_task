select market, base_date, date_bfr, sales_bfr, date_aftr, sales_aftr, 
--there is one date in the end of the year withot sales, so let it be the same as the previous day (need to replace null value)
isnull(diff_days, 1) diff_days, 
isnull(delta, 0) delta, 
--row number helps a lot when you are calculating the sales index in python
row_number() over(partition by market, diff_days, date_bfr order by base_date asc) RN
from 
(
	select a.base_date, date_bfr, sales_bfr, date_aftr, sales_aftr, 
	DATEDIFF(dd, date_bfr, date_aftr) diff_days,
	(sales_aftr-sales_bfr)/DATEDIFF(dd, date_bfr, date_aftr) delta,
	1 as market
	from
		(select base_date, date_bfr, [market_1] as sales_bfr from 
			(select date_, [market_1]  from [dbo].[sales_train]
				where [market_1] is not null) sales
		inner join
			(select base_date, max(date_bfr) date_bfr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_1] is null) base
			left join 
				(select date_ date_bfr from [dbo].[sales_train]
				where [market_1] is not null) bfr
			on base_date > date_bfr
			group by base_date) bfr
		on sales.date_ = date_bfr
		)a
	left join
		(select base_date, date_aftr, [market_1] as sales_aftr from 
			(select date_, [market_1]  from [dbo].[sales_train]
				where [market_1] is not null) sales
		inner join
			(select base_date, min(date_aftr) date_aftr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_1] is null) base
			left join 
				(select date_ date_aftr from [dbo].[sales_train]
				where [market_1] is not null) aftr
			on base_date < date_aftr
			group by base_date) aftr
		on sales.date_ = date_aftr
		) b
	on a.base_date = b.base_date

	union all

	select a.base_date, date_bfr, sales_bfr, date_aftr, sales_aftr, 
	DATEDIFF(dd, date_bfr, date_aftr) diff_days,
	(sales_aftr-sales_bfr)/DATEDIFF(dd, date_bfr, date_aftr) delta,
	2 as market
	from
		(select base_date, date_bfr, [market_2] as sales_bfr from 
			(select date_, [market_2]  from [dbo].[sales_train]
				where [market_2] is not null) sales
		inner join
			(select base_date, max(date_bfr) date_bfr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_2] is null) base
			left join 
				(select date_ date_bfr from [dbo].[sales_train]
				where [market_2] is not null) bfr
			on base_date > date_bfr
			group by base_date) bfr
		on sales.date_ = date_bfr
		)a
	left join
		(select base_date, date_aftr, [market_2] as sales_aftr from 
			(select date_, [market_2]  from [dbo].[sales_train]
				where [market_2] is not null) sales
		inner join
			(select base_date, min(date_aftr) date_aftr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_2] is null) base
			left join 
				(select date_ date_aftr from [dbo].[sales_train]
				where [market_2] is not null) aftr
			on base_date < date_aftr
			group by base_date) aftr
		on sales.date_ = date_aftr
		) b
	on a.base_date = b.base_date

	union all

	select a.base_date, date_bfr, sales_bfr, date_aftr, sales_aftr, 
	DATEDIFF(dd, date_bfr, date_aftr) diff_days,
	(sales_aftr-sales_bfr)/DATEDIFF(dd, date_bfr, date_aftr) delta,
	3 as market
	from
		(select base_date, date_bfr, [market_3] as sales_bfr from 
			(select date_, [market_3]  from [dbo].[sales_train]
				where [market_3] is not null) sales
		inner join
			(select base_date, max(date_bfr) date_bfr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_3] is null) base
			left join 
				(select date_ date_bfr from [dbo].[sales_train]
				where [market_3] is not null) bfr
			on base_date > date_bfr
			group by base_date) bfr
		on sales.date_ = date_bfr
		)a
	left join
		(select base_date, date_aftr, [market_3] as sales_aftr from 
			(select date_, [market_3]  from [dbo].[sales_train]
				where [market_3] is not null) sales
		inner join
			(select base_date, min(date_aftr) date_aftr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_3] is null) base
			left join 
				(select date_ date_aftr from [dbo].[sales_train]
				where [market_3] is not null) aftr
			on base_date < date_aftr
			group by base_date) aftr
		on sales.date_ = date_aftr
		) b
	on a.base_date = b.base_date

	union all

	select a.base_date, date_bfr, sales_bfr, date_aftr, sales_aftr, 
	DATEDIFF(dd, date_bfr, date_aftr) diff_days,
	(sales_aftr-sales_bfr)/DATEDIFF(dd, date_bfr, date_aftr) delta,
	4 as market
	from
		(select base_date, date_bfr, [market_4] as sales_bfr from 
			(select date_, [market_4]  from [dbo].[sales_train]
				where [market_4] is not null) sales
		inner join
			(select base_date, max(date_bfr) date_bfr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_4] is null) base
			left join 
				(select date_ date_bfr from [dbo].[sales_train]
				where [market_4] is not null) bfr
			on base_date > date_bfr
			group by base_date) bfr
		on sales.date_ = date_bfr
		)a
	left join
		(select base_date, date_aftr, [market_4] as sales_aftr from 
			(select date_, [market_4]  from [dbo].[sales_train]
				where [market_4] is not null) sales
		inner join
			(select base_date, min(date_aftr) date_aftr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_4] is null) base
			left join 
				(select date_ date_aftr from [dbo].[sales_train]
				where [market_4] is not null) aftr
			on base_date < date_aftr
			group by base_date) aftr
		on sales.date_ = date_aftr
		) b
	on a.base_date = b.base_date

	union all 

	select a.base_date, date_bfr, sales_bfr, date_aftr, sales_aftr, 
	DATEDIFF(dd, date_bfr, date_aftr) diff_days,
	(sales_aftr-sales_bfr)/DATEDIFF(dd, date_bfr, date_aftr) delta,
	5 as market
	from
		(select base_date, date_bfr, [market_5] as sales_bfr from 
			(select date_, [market_5]  from [dbo].[sales_train]
				where [market_5] is not null) sales
		inner join
			(select base_date, max(date_bfr) date_bfr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_5] is null) base
			left join 
				(select date_ date_bfr from [dbo].[sales_train]
				where [market_5] is not null) bfr
			on base_date > date_bfr
			group by base_date) bfr
		on sales.date_ = date_bfr
		)a
	left join
		(select base_date, date_aftr, [market_5] as sales_aftr from 
			(select date_, [market_5]  from [dbo].[sales_train]
				where [market_5] is not null) sales
		inner join
			(select base_date, min(date_aftr) date_aftr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_5] is null) base
			left join 
				(select date_ date_aftr from [dbo].[sales_train]
				where [market_5] is not null) aftr
			on base_date < date_aftr
			group by base_date) aftr
		on sales.date_ = date_aftr
		) b
	on a.base_date = b.base_date

	union all

	select a.base_date, date_bfr, sales_bfr, date_aftr, sales_aftr, 
	DATEDIFF(dd, date_bfr, date_aftr) diff_days,
	(sales_aftr-sales_bfr)/DATEDIFF(dd, date_bfr, date_aftr) delta,
	6 as market
	from
		(select base_date, date_bfr, [market_6] as sales_bfr from 
			(select date_, [market_6]  from [dbo].[sales_train]
				where [market_6] is not null) sales
		inner join
			(select base_date, max(date_bfr) date_bfr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_6] is null) base
			left join 
				(select date_ date_bfr from [dbo].[sales_train]
				where [market_6] is not null) bfr
			on base_date > date_bfr
			group by base_date) bfr
		on sales.date_ = date_bfr
		)a
	left join
		(select base_date, date_aftr, [market_6] as sales_aftr from 
			(select date_, [market_6]  from [dbo].[sales_train]
				where [market_6] is not null) sales
		inner join
			(select base_date, min(date_aftr) date_aftr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_6] is null) base
			left join 
				(select date_ date_aftr from [dbo].[sales_train]
				where [market_6] is not null) aftr
			on base_date < date_aftr
			group by base_date) aftr
		on sales.date_ = date_aftr
		) b
	on a.base_date = b.base_date

	union all

	select a.base_date, date_bfr, sales_bfr, date_aftr, sales_aftr, 
	DATEDIFF(dd, date_bfr, date_aftr) diff_days,
	(sales_aftr-sales_bfr)/DATEDIFF(dd, date_bfr, date_aftr) delta,
	7 as market
	from
		(select base_date, date_bfr, [market_7] as sales_bfr from 
			(select date_, [market_7]  from [dbo].[sales_train]
				where [market_7] is not null) sales
		inner join
			(select base_date, max(date_bfr) date_bfr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_7] is null) base
			left join 
				(select date_ date_bfr from [dbo].[sales_train]
				where [market_7] is not null) bfr
			on base_date > date_bfr
			group by base_date) bfr
		on sales.date_ = date_bfr
		)a
	left join
		(select base_date, date_aftr, [market_7] as sales_aftr from 
			(select date_, [market_7]  from [dbo].[sales_train]
				where [market_7] is not null) sales
		inner join
			(select base_date, min(date_aftr) date_aftr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_7] is null) base
			left join 
				(select date_ date_aftr from [dbo].[sales_train]
				where [market_7] is not null) aftr
			on base_date < date_aftr
			group by base_date) aftr
		on sales.date_ = date_aftr
		) b
	on a.base_date = b.base_date

	union all

	select a.base_date, date_bfr, sales_bfr, date_aftr, sales_aftr, 
	DATEDIFF(dd, date_bfr, date_aftr) diff_days,
	(sales_aftr-sales_bfr)/DATEDIFF(dd, date_bfr, date_aftr) delta,
	8 as market
	from
		(select base_date, date_bfr, [market_8] as sales_bfr from 
			(select date_, [market_8]  from [dbo].[sales_train]
				where [market_8] is not null) sales
		inner join
			(select base_date, max(date_bfr) date_bfr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_8] is null) base
			left join 
				(select date_ date_bfr from [dbo].[sales_train]
				where [market_8] is not null) bfr
			on base_date > date_bfr
			group by base_date) bfr
		on sales.date_ = date_bfr
		)a
	left join
		(select base_date, date_aftr, [market_8] as sales_aftr from 
			(select date_, [market_8]  from [dbo].[sales_train]
				where [market_8] is not null) sales
		inner join
			(select base_date, min(date_aftr) date_aftr  from
				(select date_ as base_date from [dbo].[sales_train]
				where [market_8] is null) base
			left join 
				(select date_ date_aftr from [dbo].[sales_train]
				where [market_8] is not null) aftr
			on base_date < date_aftr
			group by base_date) aftr
		on sales.date_ = date_aftr
		) b
	on a.base_date = b.base_date
) a
