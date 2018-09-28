CREATE PROCEDURE [dbo].[MergeTemplateGenerator]
	@schemaName NVARCHAR(MAX), --Alt ext
	@tableName NVARCHAR(MAX),
	@DSI_Database nvarchar(max)

AS
BEGIN
DECLARE
	@columnName nvarchar(MAX),
	@pk nvarchar(max),
	@sqlHeader nvarchar(max),
	@sqlUsing nvarchar(max),
	@sqlUpdate nvarchar(max),
	@sqlInsert nvarchar(max),
	@sqlFooter nvarchar(max),
	@sqlSelect nvarchar(max),
	@sqCompare nvarchar(max),
	@sqlInsertValues nvarchar(max),
	@sqlOnclause nvarchar(max),
	@dbExec nvarchar(200),
	@sqlCombined nvarchar(max)

DECLARE @ColumnsTable TABLE
(
  [name] nvarchar(500),
  PK int
)
--Bør måske udvides så den også kan oprette arc tabeller 
--Finder metadata til tabellen
declare @sql nvarchar(max)
set @sql=
'select c.COLUMN_NAME,iif(constraint_name is not null,1,0) as pk  
from DSI_Register.INFORMATION_SCHEMA.COLUMNS as c 
left join DSI_Register.INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
on c.COLUMN_NAME=pk.COLUMN_NAME and pk.TABLE_SCHEMA=''arc'' and c.TABLE_NAME=pk.TABLE_NAME
where c.TABLE_NAME='''+@tableName+''' and c.TABLE_SCHEMA='''+@schemaName+''''



insert into @ColumnsTable (name,pk)
exec(@sql)




	/*Bygger headeren*/
set @sqlHeader='CREATE PROCEDURE arc.[Update'+@tableName+']
	@DW_ID_Audit INT = 0, 
	@RowsInserted BIGINT = 0 OUTPUT,
	@RowsUpdated BIGINT = 0 OUTPUT,
	@RowsDeleted BIGINT = 0 OUTPUT
AS
BEGIN
	SET NOCOUNT ON;

	BEGIN TRY
		DECLARE @TranCounter INT = @@TRANCOUNT
		DECLARE @SavePoint Nvarchar(32) = CAST(@@PROCID AS Nvarchar(20)) + N''_'' + CAST(@@NESTLEVEL AS Nvarchar(2));

		IF @TranCounter > 0
			SAVE TRANSACTION @SavePoint
		ELSE
			BEGIN TRANSACTION
		
		SET @RowsDeleted = @@ROWCOUNT;

		MERGE arc.'+@tableName+' AS tgt
';
/*Starten på using/select i mergen*/
set @sqlUsing = '		USING (
				SELECT';
/*Starten på compare*/
set @sqCompare='			WHEN MATCHED AND
				(';
/*Starten på update*/
set @sqlUpdate = '			THEN UPDATE
				SET
'
/*Starten på insert*/
set @sqlInsert = '			WHEN NOT MATCHED THEN 
				INSERT (
'
/*Starten på values til insert*/
SET @sqlInsertValues = '				VALUES (
'
SET @sqlSelect=''
/*Starten på on clausen. Her bruges PK*/
set @sqlOnclause= 
'ON '

DECLARE columnNameCurser SCROLL CURSOR FOR select [name],PK from @ColumnsTable
-- Usfing statement
open columnNameCurser
fetch next from columnNameCurser into @columnName,@pk;
/*Looper over antal kolonner i sys tabellen*/
while(@@fetch_status = 0)
begin
	select @SQLselect = @SQLselect + '
					['+@columnName+'],'

	select @sqCompare = @sqCompare + '
					((src.['+@columnName+'] IS NOT NULL AND tgt.['+@columnName+'] IS NULL) OR (src.['+@columnName+'] IS NULL AND tgt.['+@columnName+'] IS NOT NULL) OR (src.['+@columnName+'] != tgt.['+@columnName+'])) OR'

	select @sqlUpdate = @sqlUpdate + '					
					tgt.['+@columnName+'] = src.['+@columnName+'],'
	
	select @sqlInsert = @sqlInsert + '					
				['+@columnName+'],'
    SELECT @sqlInsertValues = @sqlInsertValues + '					
					src.['+@columnName+'],'

	if(@pk=1)
	begin
		select @sqlOnclause=@sqlOnclause+'
		tgt.'+@columnName+' = src.'+@columnName +' AND'
	end
   FETCH NEXT FROM columnNameCurser INTO @columnName,@pk;
END
CLOSE columnNameCurser
DEALLOCATE columnNameCurser
/*Sammen sætter de forskellige elementer*/
select @sqlOnclause=substring(@sqlOnclause,1,len(@sqlOnclause)-3)
select @SQLselect =substring(@SQLselect,1,len(@SQLselect)-1)
select @sqCompare=substring(@sqCompare,1,len(@sqCompare)-2)
select @sqlUsing =@sqlUsing+ @SQLselect + '
				FROM ['+@schemaName+'].['+@tableName+']
  				) AS src
				'+@sqlOnclause+'
				'+@sqCompare+ '
				)';
select @sqlUpdate=@sqlUpdate+'
					tgt.[DW_ID_Audit_Insert]=@DW_ID_Audit,
					tgt.[DW_ID_Audit_Update]=@DW_ID_Audit,
					tgt.[DW_IsDeleted]=0
					'


SELECT @sqlInsert = @sqlInsert + '					
				[DW_ID_Audit_Insert],
				[DW_ID_Audit_Update],
				[DW_IsDeleted]
				)
'

SELECT @sqlInsertValues = @sqlInsertValues + '					
					@DW_ID_Audit,
					@DW_ID_Audit,
					0
				);
'

/*sidste del af sql scriptet*/
SET @sqlFooter='		SET @RowsInserted = @@ROWCOUNT;
		SET @RowsUpdated = 0;

		IF @TranCounter = 0
			COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH
		IF @TranCounter = 0
			ROLLBACK TRANSACTION
		ELSE IF XACT_STATE() = 1
			ROLLBACK TRANSACTION @SavePoint
		ELSE IF XACT_STATE() = -1
			ROLLBACK TRANSACTION;

		THROW;
	END CATCH
END
'


PRINT @sqlHeader
PRINT @sqlUsing
PRINT @sqlUpdate
PRINT @sqlInsert
print @sqlInsertValues
PRINT @sqlFooter
set @sqlCombined=@sqlHeader+@sqlUsing+@sqlUpdate+@sqlInsert+@sqlInsertValues+@sqlFooter
/*exec sp på anden db*/
SET @dbExec=@DSI_Database+N'.sys.sp_executesql'
EXEC @dbExec @sqlCombined

END
