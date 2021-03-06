CREATE PROCEDURE [dbo].[AutoSynonymsGenerator]
@inputvar_DSIdatabase NVARCHAR(100),
@inputvar_DSOdatabase NVARCHAR(100)
AS
BEGIN
DECLARE @dbExec NVARCHAR(500)
SET @dbExec=@inputvar_DSOdatabase+N'.sys.sp_executesql' --Execute koden i p� en anden databse
DECLARE @SQL NVARCHAR(MAX)
DECLARE @table_name NVARCHAR(1024)
DECLARE @NL char(2)
SET @NL=CHAR(13)+CHAR(10) --newlines
--M�ske vi skal lave en printonly
/*****************************SYNONYMS********************************************/
/*finder alle tabeller i DSI */
SET @SQL=	
N'	SELECT
		TABLE_NAME
	FROM '+@inputvar_DSIdatabase+N'.INFORMATION_SCHEMA.tables
	WHERE table_schema=''arc'''
	
	IF (OBJECT_ID ('tempdb..#tempInformation_schemaTables') IS NOT NULL) DROP TABLE #tempInformation_schemaTables;
	CREATE TABLE #tempInformation_schemaTables (table_name NVARCHAR(512))
	INSERT INTO #tempInformation_schemaTables (table_name)
	EXEC @dbExec @SQL

	DECLARE Cur CURSOR FOR
	SELECT table_name FROM #tempInformation_schemaTables

	OPEN Cur  
	FETCH NEXT FROM CUR INTO @table_name /* declare variables */

	WHILE @@FETCH_STATUS = 0

	BEGIN
		
	SET	@SQL=
/*Laver en lang liste med create statement*/
N'IF (OBJECT_ID ('''+@inputvar_DSOdatabase+'.arc.'+@table_name+''') IS NULL)'+@NL+' 
Create Synonym arc.'+@table_name+N' FOR '+@inputvar_DSIdatabase+N'.arc.'+@table_name
PRINT(@SQL)	
EXEC @dbExec @SQL
		FETCH NEXT FROM Cur INTO @table_name
	End
	CLOSE Cur
	DEALLOCATE Cur                      

END


