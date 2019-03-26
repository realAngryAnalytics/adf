


CREATE PROCEDURE ANGRY.sp_generate_advworks_data @inserts int, @updates int
        AS
        BEGIN

			DECLARE @i INT, @salesordernbr INT
			DECLARE @maxsalesorder nvarchar(20)
			DECLARE @statement nvarchar(500)
			DECLARE @currentdatetime datetime

			select @maxsalesorder = MAX(SalesOrderNumber)
			from ANGRY.FactResellerSales

			SET @currentdatetime = getdate()

			SET @statement = 	
			'UPDATE ANGRY.FactResellerSales
			SET OrderQuantity = 42, ModifiedDate = ''' + CONVERT(VARCHAR, @currentdatetime, 121) + ''' 
			WHERE SalesOrderNumber+''-''+CAST(SalesOrderLineNumber as nvarchar(4)) in
				(select top ' + CAST(@updates as nvarchar(5)) + ' SalesOrderNumber+''-''+CAST(SalesOrderLineNumber as nvarchar(4))
					FROM ANGRY.FactResellerSales tablesample(10 percent) )'

			execute sp_executesql @statement

			SELECT @salesordernbr = CAST(SUBSTRING( @maxsalesorder,3,LEN(@maxsalesorder)-2) as int)
			SET @salesordernbr = @salesordernbr + 1

			SET @i = 1
			WHILE (@i <= @inserts)
			BEGIN
				
				INSERT INTO ANGRY.FactResellerSales
				VALUES(0,0,0,0,0,0,0,0,0,'SO'+CAST(@salesordernbr as nvarchar(18)),1,0,1,4.99,4.99,0,0,4.99,4.99,4.99,0,0,'0','0',@currentdatetime,@currentdatetime,@currentdatetime,@currentdatetime)

				SET @salesordernbr = @salesordernbr + 1
				SET @i = @i + 1
			END



		END


--exec ANGRY.sp_generate_advworks_data 1000, 500
--select count(*) from Angry.FactResellerSales where ModifiedDate = (select MAX(ModifiedDate) from ANGRY.FactResellerSales);







