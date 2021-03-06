/****** Script for SelectTopNRows command from SSMS  ******/
SELECT cast([date] as date) [date_],
case when [market_1] = 'NA' then Null
		else cast([market_1] as float) 
		end [market_1],
case when [market_2] = 'NA' then Null
		else cast([market_2] as float) 
		end [market_2],
case when [market_3] = 'NA' then Null
		else cast([market_3] as float)
		end [market_3],
case when [market_4] = 'NA' then Null
		else cast([market_4] as float)
		end [market_4],
case when [market_5] = 'NA' then Null
		else cast([market_5] as float)
		end [market_5],
case when [market_6] = 'NA' then Null
		else cast([market_6] as float)
		end [market_6],
case when [market_7] = 'NA' then Null
		else cast([market_7] as float)
		end [market_7],
case when [market_8] = 'NA' then Null
		else cast([market_8] as float)
		end [market_8]
into [S7].[dbo].[sales_train]
FROM [S7].[dbo].[sales_train_raw];

SELECT cast([date] as date) [date_]
      ,cast([market_1] as tinyint) [market_1]
      ,cast([market_2] as tinyint) [market_2]
      ,cast([market_3] as tinyint) [market_3]
      ,cast([market_4] as tinyint) [market_4]
      ,cast([market_5] as tinyint) [market_5]
      ,cast([market_6] as tinyint) [market_6]
      ,cast([market_7] as tinyint) [market_7]
      ,cast([market_8] as tinyint) [market_8]
into [S7].[dbo].[advert_train]
FROM [S7].[dbo].[advert_train_raw];

SELECT cast([date] as date) [date_]
      ,cast([market_1] as tinyint) [market_1]
      ,cast([market_2] as tinyint) [market_2]
      ,cast([market_3] as tinyint) [market_3]
      ,cast([market_4] as tinyint) [market_4]
      ,cast([market_5] as tinyint) [market_5]
      ,cast([market_6] as tinyint) [market_6]
      ,cast([market_7] as tinyint) [market_7]
      ,cast([market_8] as tinyint) [market_8]
into [S7].[dbo].[advert_test]
FROM [S7].[dbo].[advert_test_raw];

