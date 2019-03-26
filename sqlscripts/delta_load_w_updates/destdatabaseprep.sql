/* Destination database preperation */
CREATE SCHEMA ANGRY;
GO

/* Part of the original delta load solution */
CREATE TABLE [ANGRY].[watermarktable](
	[TableName] [varchar](255) NULL,
	[WatermarkValue] [datetime] NULL
) 
WITH  
  (   
    CLUSTERED INDEX (TableName)  
  ); 
GO

/* Initiallize the watermarktable to pickup all records on the inital run */
INSERT INTO ANGRY.watermarktable
VALUES('ANGRY.FactResellerSales','01/01/1900 00:00:00')
GO

/* Part of the original delta load solution, slightly altered to include a TableName */
CREATE PROCEDURE ANGRY.update_watermark @LastModifiedtime datetime, @TableName varchar(255)
        AS
        BEGIN
            UPDATE ANGRY.watermarktable
            SET [WatermarkValue] = @LastModifiedtime 
			WHERE [TableName] = @TableName
        END

GO


/* Create the destination table that will be synced from the source table */
  CREATE TABLE [ANGRY].[FactResellerSales](
	[ProductKey] [int] NOT NULL,
	[OrderDateKey] [int] NOT NULL,
	[DueDateKey] [int] NOT NULL,
	[ShipDateKey] [int] NOT NULL,
	[ResellerKey] [int] NOT NULL,
	[EmployeeKey] [int] NOT NULL,
	[PromotionKey] [int] NOT NULL,
	[CurrencyKey] [int] NOT NULL,
	[SalesTerritoryKey] [int] NOT NULL,
	[SalesOrderNumber] [nvarchar](20) NOT NULL,
	[SalesOrderLineNumber] [tinyint] NOT NULL,
	[RevisionNumber] [tinyint] NULL,
	[OrderQuantity] [smallint] NULL,
	[UnitPrice] [money] NULL,
	[ExtendedAmount] [money] NULL,
	[UnitPriceDiscountPct] [float] NULL,
	[DiscountAmount] [float] NULL,
	[ProductStandardCost] [money] NULL,
	[TotalProductCost] [money] NULL,
	[SalesAmount] [money] NULL,
	[TaxAmt] [money] NULL,
	[Freight] [money] NULL,
	[CarrierTrackingNumber] [nvarchar](25) NULL,
	[CustomerPONumber] [nvarchar](25) NULL,
	[OrderDate] [datetime] NULL,
	[DueDate] [datetime] NULL,
	[ShipDate] [datetime] NULL
)
WITH ( CLUSTERED COLUMNSTORE INDEX )
GO

/* A staging table will be required for this */
CREATE TABLE [ANGRY].[FactResellerSales_Staging](
	[ProductKey] [int] NOT NULL,
	[OrderDateKey] [int] NOT NULL,
	[DueDateKey] [int] NOT NULL,
	[ShipDateKey] [int] NOT NULL,
	[ResellerKey] [int] NOT NULL,
	[EmployeeKey] [int] NOT NULL,
	[PromotionKey] [int] NOT NULL,
	[CurrencyKey] [int] NOT NULL,
	[SalesTerritoryKey] [int] NOT NULL,
	[SalesOrderNumber] [nvarchar](20) NOT NULL,
	[SalesOrderLineNumber] [tinyint] NOT NULL,
	[RevisionNumber] [tinyint] NULL,
	[OrderQuantity] [smallint] NULL,
	[UnitPrice] [money] NULL,
	[ExtendedAmount] [money] NULL,
	[UnitPriceDiscountPct] [float] NULL,
	[DiscountAmount] [float] NULL,
	[ProductStandardCost] [money] NULL,
	[TotalProductCost] [money] NULL,
	[SalesAmount] [money] NULL,
	[TaxAmt] [money] NULL,
	[Freight] [money] NULL,
	[CarrierTrackingNumber] [nvarchar](25) NULL,
	[CustomerPONumber] [nvarchar](25) NULL,
	[OrderDate] [datetime] NULL,
	[DueDate] [datetime] NULL,
	[ShipDate] [datetime] NULL
)
WITH ( HEAP );
GO

/* The above FactResellerSales table is the original Adventure Works table. 
   However, to handle the scenario of updates, we need to include a modfied timestamp */
ALTER TABLE [ANGRY].[FactResellerSales]
ADD ModifiedDate datetime null
GO

ALTER TABLE [ANGRY].[FactResellerSales_Staging]
ADD ModifiedDate datetime null
GO

/* Initially set the new modified date to the oder date */
UPDATE [ANGRY].[FactResellerSales]
SET ModifiedDate = OrderDate
GO



/* This is specific to the concept of being able to handle delta loads 
	that may contain updates to previously loaded data */
CREATE TABLE [ANGRY].[deltacontroltable](
	[TableName] [varchar](255) NULL,
	[WatermarkColumn] [varchar](255) NULL,
	[KeyColumn1] [varchar](255) NULL,
	[KeyColumn2] [varchar](255) NULL,
	[KeyColumn3] [varchar](255) NULL,
	[KeyColumn4] [varchar](255) NULL,
	[KeyColumn5] [varchar](255) NULL,
	[KeyColumn6] [varchar](255) NULL
) 
WITH  
  (   
    CLUSTERED INDEX (TableName)  
  )
GO

  
/* Insert into detlacontroltable the metadata about the destination table and the key fields that are needed to 
	get a unique record. For FactResellerSales this is SalesOrderNumber and SalesOrderLineNumber */
INSERT INTO ANGRY.deltacontroltable
values ('ANGRY.FactResellerSales','ModifiedDate','SalesOrderNumber','SalesOrderLineNumber',null,null,null,null)
GO

/* TODO: null checking, transaction */
CREATE PROCEDURE ANGRY.delta_load_w_updates @LastModifiedDatetime datetime, @TableName varchar(255), @StagingTableName varchar(255)
        AS
        BEGIN

			declare @KeyColumn1 varchar(255), @KeyColumn2 varchar(255), @KeyColumn3 varchar(255), @KeyColumn4 varchar(255), 
					@KeyColumn5 varchar(255), @KeyColumn6 varchar(255), @KeyColumn varchar(255)
			declare @statement nvarchar(4000), @section1 nvarchar(1000), @section2 nvarchar(1000), @section3 nvarchar(1000)
			declare @return int, @keys int, @i int

			select @KeyColumn1 = KeyColumn1, @KeyColumn2 = KeyColumn2, @KeyColumn3 = KeyColumn3, 
					@KeyColumn4 = KeyColumn4, @KeyColumn5 = KeyColumn5, @KeyColumn6 = KeyColumn6
			from [ANGRY].[deltacontroltable]
			where TableName = @TableName

			IF (@KeyColumn6 IS NOT NULL) BEGIN SET @keys = 6 END 
			ELSE IF @KeyColumn5 is not null BEGIN SET @keys = 5 END
			ELSE IF @KeyColumn4 is not null BEGIN SET @keys = 4 END
			ELSE IF @KeyColumn3 is not null BEGIN SET @keys = 3 END
			ELSE IF @KeyColumn2 is not null BEGIN SET @keys = 2 END
			ELSE IF @KeyColumn1 is not null BEGIN SET  @keys = 1 END

			SET @section1 = ''
			SET @section2 = ''
			SET @section3 = ''
			SET @i = 1

			WHILE (@i <= @keys)
			BEGIN

				IF @i = 1 BEGIN SET @KeyColumn = @KeyColumn1  END 
				ELSE IF @i = 2 BEGIN SET @KeyColumn = @KeyColumn2  END
				ELSE IF @i = 3 BEGIN SET @KeyColumn = @KeyColumn3  END
				ELSE IF @i = 4 BEGIN SET @KeyColumn = @KeyColumn4  END
				ELSE IF @i = 5 BEGIN SET @KeyColumn = @KeyColumn5  END
				ELSE IF @i = 6 BEGIN SET @KeyColumn = @KeyColumn6  END

				SET @section1 = @section1 + ' CAST(' + @KeyColumn + ' as varchar(255))'
				IF @i <> @keys BEGIN SET @section1 = @section1 + ' + ''|'' + ' END

				SET @section2 = @section2 + ' CAST(t2.' + @KeyColumn + ' as varchar(255))'
				IF @i <> @keys BEGIN SET @section2 = @section2 + ' + ''|'' + ' END

				SET @section3 = @section3 + ' t2.' + @KeyColumn + ' = t1.' + @KeyColumn
				IF @i <> @keys BEGIN SET @section3 = @section3 + ' AND ' END

				SET @i = @i + 1
			END
			
			
			SET @statement = 'DELETE ' + @TableName + ' WHERE ' + @section1 + ' IN (SELECT ' + @section2 + 
								' FROM ' + @StagingTableName + ' t2' +
								' INNER JOIN ' + @TableName + ' t1 ON ' + @section3 + ')'
			--select @statement

			EXECUTE sp_executesql @statement

			SET @statement = 'INSERT INTO ' + @TableName + ' SELECT * FROM ' + @StagingTableName
			--select @statement

			EXECUTE sp_executesql @statement

			exec ANGRY.update_watermark @LastModifiedDatetime, @TableName

		

        END
GO

--exec ANGRY.delta_load_w_updates '11/29/2013 00:00:00', 'ANGRY.FactResellerSales', 'ANGRY.FactResellerSales_Staging'
		
/* Example output of the stored procedure */
--DELETE ANGRY.FactResellerSales 
--WHERE  CAST(SalesOrderNumber as varchar(255)) + '|' +  CAST(SalesOrderLineNumber as varchar(255)) 
--IN (SELECT  CAST(t2.SalesOrderNumber as varchar(255)) + '|' +  CAST(t2.SalesOrderLineNumber as varchar(255)) 
--		FROM ANGRY.FactResellerSales_Staging t2 
--		INNER JOIN ANGRY.FactResellerSales t1 ON  t2.SalesOrderNumber = t1.SalesOrderNumber AND  t2.SalesOrderLineNumber = t1.SalesOrderLineNumber)



