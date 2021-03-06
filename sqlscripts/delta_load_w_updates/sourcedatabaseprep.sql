

/* Going to create a new schema and FactResellerSales table so that we do not alter the adventureworks dw example datasets */
  CREATE SCHEMA ANGRY
  GO

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
	[ShipDate] [datetime] NULL,
)
GO

/* populate the table from the original dataset */
INSERT INTO ANGRY.FactResellerSales
SELECT * FROM dbo.FactResellerSales
GO


/* The above FactResellerSales table is the original Adventure Works table. 
   However, to handle the scenario of updates, we need to include a modfied timestamp */
ALTER TABLE [ANGRY].[FactResellerSales]
ADD ModifiedDate datetime null
GO

/* Initially set the new modified date to the oder date */
UPDATE [ANGRY].[FactResellerSales]
SET ModifiedDate = OrderDate
GO


