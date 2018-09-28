CREATE PROCEDURE [dbo].[AutofctKodeGenerator]
@inputvar_DSOdatabase NVARCHAR(100),
@inputvar_kildeNavn NVARCHAR(100),
@FctTableName NVARCHAR(200),
@InputFctColumnDTypeList NVARCHAR(MAX),
@PrintOnly bit 
AS
BEGIN


/* HOW TO:
AutoFctKodeGenerator skal køres fra dwFramework.
@inputvar_DSOdatabase er databasen hvor facten skal ende.
@InputFctColumnDTypeList angiver kolonner og datatyper og har samme syntax som indholdet i et create statement. 
	Navnene ksal være nøjagtige samme navngivning som forventet i slut facten
	Den opretter dog selv dw_sk_dato og dw_sk_tidspunkt hvis man tilføjer en kolonner med navn og type [datotid_rolle] [Datetime2](3)
	Alle kolonnetyper kan bruges.
	Der skal være [] omkring datatyperne og kolonnenavnet ( dette scriptes ofte ud af create statementet).	
	Angiv én primærnøgle i kolonnelisten med /*primarykey*/. Det skal altid være én DW_SK_(Samme navn som facten). Så der skal være en transactions dimension med multitenancy=2.
	Den opretter selv e kolonne med 1 i alle rækker  
	
@printonly=1 printer alle statement til messages. 
@printonly=0 execute med det samme ( pas på med multitenancy og shareddimension.)
/*logik start*/ og /*logik slut*/ -tag i alle storedprocedures. Her kan man lave userdefined logik, som  ved en ny execute af AutofctKodeGenerator overfører til det nye CREATE statement af storedprocedurene. 
Når storedproceduren er kørt succesful, så put ETL-logik i SP stg.InsertfctXXX_kilde. Input til stg SP er dw_ek nøgler for alle dimensioner, samt measuress. Se en hvilkensom helst stg.InsertFctXXX_Kilde
--Vil multiple input til en fact så husk at samle inputs i fct.UpdateXXX. Her findes et logik tag til det.
*/


/* WHAT DOES IT DO
Opretter stg og fct tables
Opretter stg og fct Storedprocedures
*/
DECLARE @Original_InputFctColumnDTypeList NVARCHAR(MAX)
SET @Original_InputFctColumnDTypeList=@InputFctColumnDTypeList
DECLARE @Counter INT
SET @Counter = 0
DECLARE @TotalPrints INT
DECLARE @selectsql NVARCHAR(MAX)
DECLARE @joinsql NVARCHAR(MAX)
DECLARE @SQL NVARCHAR(MAX)
DECLARE @FctColumnDTypeList NVARCHAR(MAX)
DECLARE @NL char(2)
SET @NL=CHAR(13)+CHAR(10)
DECLARE @stgFilegroup nvarchar(200)
DECLARE	@modelFilegroup NVARCHAR(200)
SET @stgFilegroup='Staging'
SET @modelFilegroup='Standard'
DECLARE @dbExec NVARCHAR(100)
SET @dbExec=@inputvar_DSOdatabase+N'.sys.sp_executesql'


/*Går i gang med at kigge på decimalerne. De driller pga , i f.eks. (18,2)*/
DECLARE @pos INT
DECLARE @Oldpos INT
DECLARE @subs NVARCHAR(max)
SET @pos=0
SET @oldpos=0

IF @InputFctColumnDTypeList like '%[DECIMAL](%'
BEGIN
	WHILE @pos >= @oldpos
		begin
		
	/*Finder næste decimal*/		
			SET @subs= SUBSTRING(@InputFctColumnDTypeList,CHARINDEX('[DECIMAL](',@InputFctColumnDTypeList,@pos),CHARINDEX(')',@InputFctColumnDTypeList,CHARINDEX('[DECIMAL](',@InputFctColumnDTypeList,@pos)) -CHARINDEX('[DECIMAL](',@InputFctColumnDTypeList,@pos)+1 )
		
	/*Replace alle occurences med samme decimal presicion*/
	
			SET @InputFctColumnDTypeList= REPLACE(@InputFctColumnDTypeList,@subs, Replace(@subs,',',';'))
		
		/*Sætter postition på nuværende decimal, så vi kan søge frem efter den næste decimal*/	
			SET @Oldpos=CHARINDEX('[DECIMAL](',@InputFctColumnDTypeList,@pos)+CHARINDEX(')',@InputFctColumnDTypeList,CHARINDEX('[DECIMAL](',@InputFctColumnDTypeList,@pos)) -CHARINDEX('[DECIMAL](',@InputFctColumnDTypeList,@pos)
			SET @pos =CHARINDEX('[DECIMAL](',@InputFctColumnDTypeList,@Oldpos)


		END
END		

/*Laver datotid om til kolonnerne datotid, DW_SK_Dato og DW_SK_Tidspunkt*/
SELECT @fctColumnDTypeList= COALESCE(@fctColumnDTypeList+','+@NL, '') +REPLACE(IIF(col IS NOT null,c.col,l.value),@NL,'')
FROM  String_Split(@InputFctColumnDTypeList,',') l
OUTER APPLY (SELECT Col FROM (	SELECT l.Value AS Col
								UNION ALL
								SELECT '[DW_SK_Dato'+COALESCE(REPLACE(SUBSTRING(l.value,CHARINDEX('[Datotid',l.value),CHARINDEX(']',l.value)-CHARINDEX('[Datotid',l.value)),'[Datotid',''),'')+']'+' [INT] NOT NULL' AS Col
								UNION ALL 
								SELECT '[DW_SK_Tidspunkt'+COALESCE(REPLACE(SUBSTRING(l.value,CHARINDEX('[Datotid',l.value),CHARINDEX(']',l.value)-CHARINDEX('[Datotid',l.value)),'[Datotid',''),'')+']'+' [INT] NOT NULL' AS Col
						) AS c WHERE l.value LIKE '%datotid%' and l.value not like '%[/][*][[]datotid%'

					) AS c


Declare  @fctColumnList  NVARCHAR(MAX)
SELECT  @fctColumnList = COALESCE(@fctColumnList + ',', '') + SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) +@NL
FROM  String_Split(@fctColumnDTypeList,',') l
--SELECT @fctColumnList


Declare  @fctColumnListStg  NVARCHAR(MAX);
Select @fctColumnListStg = COALESCE(@fctColumnListStg + ',', '') + l.value
FROM  String_Split(@fctColumnDTypeList,',') l
WHERE value NOT LIKE '%DW_SK_%'


DECLARE @fctColumnListeUpdate NVARCHAR(MAX);
Select @fctColumnListeUpdate = COALESCE(@fctColumnListeUpdate + ',', '') + '[target].'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) +'= [source].'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) +@NL
FROM  String_Split(@fctColumnDTypeList,',') AS l
WHERE l.value NOT LIKE '%Primarykey%'


DECLARE @fctPKColumn NVARCHAR(MAX);
Select @fctPKColumn =SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))
FROM  String_Split(@fctColumnDTypeList,',') AS l
WHERE value LIKE '%PrimaryKey%'


IF (OBJECT_ID ('tempdb..#tempMap') IS NOT NULL) drop table #tempMap
SELECT 
	COL
	,mapCol=IIF(pos=0,SUBSTRING(col,2,LEN(col)-2),SUBSTRING(col,2,pos-2))
	,maptableName=REPLACE(IIF(pos=0,SUBSTRING(col,2,LEN(col)-2),SUBSTRING(col,2,pos-2)),'DW_EK_','')+'_'+@inputvar_kildeNavn
	,REPLACE(COL,'DW_EK_','') AS rolle
	,REPLACE(IIF(pos=0,SUBSTRING(col,2,LEN(col)-2),SUBSTRING(col,2,pos-2)),'DW_EK_','') AS dimensionName
	,HistDatotid
	INTO #tempMap
	FROM(
	SELECT 
		REPLACE(SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)),'DW_SK_','DW_EK_') AS Col
		,CHARINDEX('_',SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))
				,1+LEN('[DW_SK_')) AS pos --Starter efter dw_sk_
		,IIF(value NOT like '%primarykey%',  substring(value,2+CHARINDEX('/*',value),CHARINDEX('*/',value)-CHARINDEX('/*',value)-2),null) as HistDatotid
	FROM  String_Split(@fctColumnDTypeList,',')	 
	WHERE value LIKE '%DW_SK_%' AND NOT (value LIKE '%DW_SK_Dato%' OR value LIKE '%DW_SK_Tidspunkt%') ) AS maptable







/*****************************TABLES********************************************/

/*Fact*/
SET @sql =
N'IF (OBJECT_ID (''fct.'+@fctTableName+''') IS NOT NULL) drop table fct.'+@fctTableName+''+@NL+
'
/****** Object:  Table [fct].['+@fctTableName+']    Script Date: '+CAST(GETDATE() AS NVARCHAR(30))+' ******/'+@NL+'
SET ANSI_NULLS ON


SET QUOTED_IDENTIFIER ON


CREATE TABLE [fct].['+@fctTableName+'](
	'+REPLACE(@fctColumnDTypeList,';',',')+',
	[AntalRaekker] [Bit] NOT NULL,
	[DW_ID_Audit_Insert] [int] NOT NULL,
	[DW_ID_Audit_Update] [int] NOT NULL,
CONSTRAINT [PK_'+@fctTableName+'] PRIMARY KEY NONCLUSTERED 
(
	[DW_SK_'+@fctTableName+'] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, DATA_COMPRESSION = ROW) ON ['+@modelFilegroup+']
) ON ['+@modelFilegroup+']
WITH
(
DATA_COMPRESSION = ROW
)

ALTER AUTHORIZATION ON [fct].['+@fctTableName+'] TO  SCHEMA OWNER 

ALTER TABLE [fct].['+@fctTableName+'] ADD  CONSTRAINT [DF_F'+@fctTableName+'_AuditInsert]  DEFAULT ((0)) FOR [DW_ID_Audit_Insert]
ALTER TABLE [fct].['+@fctTableName+'] ADD  CONSTRAINT [DF_F'+@fctTableName+'_AuditUpdate]  DEFAULT ((0)) FOR [DW_ID_Audit_Update]
ALTER TABLE [fct].['+@fctTableName+'] ADD  CONSTRAINT [DF_F'+@fctTableName+'_AntalRaekker]  DEFAULT ((1)) FOR [AntalRaekker]
'


SET @TotalPrints = (LEN(@sql) / 4000) + 1
SET @Counter=0
WHILE @Counter < @TotalPrints 
BEGIN
	PRINT(SUBSTRING(@sql,1+@Counter*4000,@Counter*4000+4000 ))
    SET @Counter = @Counter + 1
END


IF(@PrintOnly=0) 
BEGIN 
EXEC @dbExec @SQL
END

/*Stg*/

SET @sql =
N'IF (OBJECT_ID ('''+@inputvar_DSOdatabase+'.stg.fct'+@fctTableName+'_'+@inputvar_kildeNavn+''') IS NOT NULL) drop table stg.fct'+@fctTableName+'_'+@inputvar_kildeNavn+''+@NL+
'
/****** Object:  Table [stg].[fct'+@fctTableName+'_'+@inputvar_kildeNavn+']    Script Date: '+CAST(GETDATE() AS NVARCHAR(30))+' ******/'+CHAR(10)+'

CREATE TABLE [stg].[fct'+@fctTableName+'_'+@inputvar_kildeNavn+'](
	'
	+REPLACE(REPLACE(@fctColumnDTypeList,';',','),'DW_SK_','DW_EK_')+'
 ) ON ['+@stgFilegroup+']

SET ANSI_PADDING ON
'


SET @TotalPrints = (LEN(@sql) / 4000) + 1

SET @Counter=0
WHILE @Counter < @TotalPrints 
BEGIN
	PRINT(SUBSTRING(@sql,1+@Counter*4000,@Counter*4000+4000 ))
    SET @Counter = @Counter + 1
END


IF(@PrintOnly=0) 
BEGIN 
EXEC @dbExec @SQL
END




/*****************************StoredProcedure********************************************/
/*Staging*/
DECLARE @logik nvarchar(MAX)

SET @logik= '
CREATE TABLE #TEMP
(
'+REPLACE(REPLACE(@FctColumnDTypeList,';',','),'DW_SK_','DW_EK_')+
')'

	
	IF(OBJECT_ID(N''+@inputvar_DSOdatabase+'.stg.Selectfct'+@fctTableName+'_'+@inputvar_kildeNavn) IS NOT NULL)
	BEGIN
		
		DECLARE @def NVARCHAR(MAX)
			Set @SQL = N'SELECT @def= OBJECT_DEFINITION( OBJECT_ID(N'''+@inputvar_DSOdatabase+'.stg.Selectfct'+@fctTableName+'_'+@inputvar_kildeNavn+'''))'
		exec @dbExec @SQL,N'@def nvarchar(max) out', @def out
	
	
		SELECT @logik=SUBSTRING(@def
			,LEN('/*LOGIK START*/')+CHARINDEX('/*LOGIK START*/',@def)
			,-(1+LEN('/*LOGIK SLUT*/'))+CHARINDEX('/*LOGIK SLUT*/',@def)-CHARINDEX('/*LOGIK START*/',@def)
			)

	
	END  
	
	
	
SELECT @selectsql =COALESCE(@selectsql + ',', '') + col 	
FROM (
	select
				  CASE 
				WHEN value LIKE '%Datotid%' THEN 'CAST( source.'+l.value +' AS Datetime2(3)) AS '+l.value
				WHEN value LIKE '%DW_SK_Dato%' THEN 'Coalesce('+COALESCE(@selectsql + ',', '') +'YEAR(CAST( source.'+Replace(RIGHT(l.value,CHARINDEX('_',REVERSE(l.value))),'_','[Datotid_') +' AS Datetime2(3)))*10000+MONTH(CAST( source.'+Replace(RIGHT(l.value,CHARINDEX('_',REVERSE(l.value))),'_','[Datotid_') +' AS Datetime2(3)))*100+day(CAST( source.'+Replace(RIGHT(l.value,CHARINDEX('_',REVERSE(l.value))),'_','[Datotid_') +' AS Datetime2(3))),-1) AS '+l.value
				WHEN VALUE LIKE '%DW_SK_tidspunkt%' THEN 'Coalesce(DATEPART(Hour, CAST(source.'+Replace(RIGHT(l.value,CHARINDEX('_',REVERSE(l.value))),'_','[Datotid_') +' AS Datetime2(3)))*60+DATEPART(MINUTE, CAST( source.'+Replace(RIGHT(l.value,CHARINDEX('_',REVERSE(l.value))),'_','[Datotid_') +'   AS Datetime2(3))),-1) AS '+l.value
				WHEN value NOT LIKE '%DW_SK_%' AND NOT (l.value LIKE '%Datotid%' OR l.value LIKE '%DW_SK_tidspunkt%' OR l.value LIKE '%DW_SK_Dato%') THEN 'source.'+Replace(SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)),@NL,'')
				WHEN value LIKE '%DW_SK%' THEN REPLACE(l.value,'DW_SK_','DW_EK_')
				ELSE 'fejl '+ value
				END
				AS col
	FROM  String_Split(@fctColumnList,',') l
	LEFT JOIN #tempMap AS m
	ON m.Col=SUBSTRING(REPLACE(l.value,'DW_SK_','DW_EK_'),1,CHARINDEX(']',l.value))) AS sub


set @selectsql ='Select 
	'+Replace(@selectsql,'DW_SK_','DW_EK_') +
	'
	FROM #TEMP as source
	'

	
SET @SQL=
'
/****** Object:  StoredProcedure [stg].[Selectfct'+@fctTableName+'_'+@inputvar_kildeNavn+']    Script Date: 22-12-2017 10:16:35 ******/
'+IIF(OBJECT_ID (N''+@inputvar_DSOdatabase+'.stg.Selectfct'+@fctTableName+'_'+@inputvar_kildeNavn) IS NULL,'CREATE','ALTER') +' PROCEDURE [stg].[Selectfct'+@fctTableName+'_'+@inputvar_kildeNavn+']
@DW_ID_Audit_EndValue_Last INT = 0,
@DW_ID_Audit INT =0
AS
	SET NOCOUNT ON
	

	SET @DW_ID_Audit_EndValue_Last=ISNULL(@DW_ID_Audit_EndValue_Last,0);
/*LOGIK START*/'
+@logik+
'/*LOGIK SLUT*/
	/*Udskift Create Temp staetment med al logik
	husk delta WHERE [DW_ID_Audit_Update]>=@DW_ID_Audit_EndValue_Last */


	TRUNCATE TABLE [stg].[fct'+@fctTableName+'_'+@inputvar_kildeNavn+'];

	INSERT INTO stg.fct'+@fctTableName+'_'+@inputvar_kildeNavn+' (
	'+REPLACE(Replace(@fctColumnList,'DW_SK_','DW_EK_'),',',CHAR(9)+',')+'
	)
	'
	+Replace(@selectsql,'DW_SK_','DW_EK_')+
	'

	UPDATE STATISTICS [stg].[fct'+@fctTableName+'_'+@inputvar_kildeNavn+'];

/*last run with Auto:

	DECLARE	@return_value int

EXEC	@return_value = [dbo].[AutoFctKodeGenerator]
		@inputvar_DSOdatabase = N'''+@inputvar_DSOdatabase+''',
		@inputvar_kildeNavn = N'''+@inputvar_kildeNavn+''',
		@fctTableName = N'''+@FctTableName+''',
		@InputFctColumnDTypeList = 
N'''+@Original_InputFctColumnDTypeList+
''',
		@PrintOnly = '+cast(@PrintOnly as nvarchar(1))+'

SELECT	''Return Value'' = @return_value
	
*/

'


SET @TotalPrints = (LEN(@sql) / 4000) + 1
SET @Counter=0
WHILE @Counter < @TotalPrints 
BEGIN
	PRINT(SUBSTRING(@SQL,1+@Counter*4000,@Counter*4000+4000 ))
    SET @Counter = @Counter + 1
END


IF(@PrintOnly=1) 
BEGIN 
EXEC @dbExec @SQL
END



/*fct*/
SET @Joinsql=null
SET @selectsql=null
SELECT @Joinsql =COALESCE(@joinsql + '', '') +  
		'LEFT JOIN dim.'+m.dimensionName+' AS '+m.rolle+ ' WITH (NOLOCK) ON ('+m.rolle+'.'+m.mapCol+'= source.'+m.Col+IIF(m.histDatotid IS NULL,'',' AND source.'+m.histDatotid+' >= '+m.rolle+'.DW_ValidFrom AND source.'+m.histDatotid+' < '+m.rolle+'.DW_ValidTo')+')'+@NL
	FROM #tempMap AS m


SELECT @selectsql =COALESCE(@selectsql + ',', '') +  
		CASE	WHEN tm.col IS NOT NULL THEN 'ISNULL('+tm.rolle+'.'+replace(tm.mapCol,'DW_EK_','DW_SK_')+',-1) AS '+replace(tm.col,'DW_EK_','DW_SK_')
				WHEN l.value LIKE '%DW_SK_%' THEN 'ISNULL(source.'+REPLACE(SUBSTRING( l.value,CHARINDEX('[',l.value,1),1+CHARINDEX(']',l.value,1)-CHARINDEX('[',l.value,1)),'DW_SK_','DW_EK_')+',-1) AS '+SUBSTRING( l.value,CHARINDEX('[',l.value,1),1+CHARINDEX(']',l.value,1)-CHARINDEX('[',l.value,1))

		ELSE 'source.'+SUBSTRING( l.value,CHARINDEX('[',l.value,1),1+CHARINDEX(']',l.value,1)-CHARINDEX('[',l.value,1))
		END + @NL
FROM  String_Split(@fctColumnDTypeList,',') l
LEFT JOIN #tempMap AS tm
ON replace(tm.col,'DW_EK_','DW_SK_')=SUBSTRING( l.value,CHARINDEX('[',l.value,1),1+CHARINDEX(']',l.value,1)-CHARINDEX('[',l.value,1))

	
DECLARE @SQLReadjustBorger NVARCHAR(MAX)
DECLARE @readjustdate NVARCHAR(MAX)
SET @SQLReadjustBorger=''
SET @readjustdate=(select HistDatotid
	FROM  #tempMap l
	WHERE l.Col LIKE '%DW_SK_Borger%')
	
IF(@readjustdate IS NOT NULL)
	BEGIN
		SET @SQLReadjustBorger=
		'exec [dim].[UpdateFact_Readjust_SK_Borger] 
			@DatabaseName = @DBNAME,
			@SchemaName = ''fct'',
			@TableName = '''+@FctTableName+''',
			@DW_SK_Borger_ColumnName = ''DW_SK_Borger'',
			@DW_EK_Borger_ColumnName = ''DW_EK_Borger'',
			@DW_Lookup_Dato_ColumnName = '''+@readjustdate+''',
			@DW_ID_Audit = @DW_ID_Audit,
			@DW_ID_Audit_EndValue_Last = @DW_ID_Audit_EndValue_Last,
			@RowsUpdated = @RowsUpdated OUTPUT;'
		
	END


SET @sql=N'
'+IIF(OBJECT_ID (N''+@inputvar_DSOdatabase+'.fct.Update'+@fctTableName) IS NULL,'CREATE','ALTER') +' PROCEDURE [fct].[Update'+@fctTableName+']
@DW_ID_Audit INT = 1, 
@DW_ID_Audit_EndValue_Last INT = 0,
@RowsInserted BIGINT = 0 OUTPUT , 
@RowsUpdated BIGINT = 0 OUTPUT , 
@RowsDeleted BIGINT = 0 OUTPUT 
AS SET NOCOUNT ON; 
BEGIN TRY 

	-- Get the current transaction count 
	DECLARE @TranCounter INT = @@TRANCOUNT , @SavePoint NVARCHAR(32) = CAST(@@PROCID AS NVARCHAR(20)) + N''_'' + CAST(@@NESTLEVEL AS NVARCHAR(2)); 
	DECLARE @rowcounts TABLE(mergeAction nvarchar(10));
		SET @RowsInserted = 0
		SET @RowsUpdated = 0
		SET @RowsDeleted = 0
		SET @DW_ID_Audit = ISNULL(@DW_ID_Audit, 0)
		SET @DW_ID_Audit_EndValue_Last = ISNULL(@DW_ID_Audit_EndValue_Last, 0)
	-- Midlertidig tabel der benyttes i forbindelse med isnull og join til dim for at få dw_skere
	

	/* Begin updating fact table*/ 
	IF (OBJECT_ID(''tempdb..#stg'') IS NOT NULL) DROP TABLE #stg;
	
		SELECT
		'+REPLACE(@selectsql,',',CHAR(9)+CHAR(9)+',')+'
		INTO #stg
		FROM [stg].[fct'+@fctTableName+'_'+@inputvar_kildeNavn+'] source
		'
		+@joinsql+
		' 

	create nonclustered index idx_tmp_upd_'+@fctTableName+' on #stg('+@fctPKColumn+')

	DECLARE @DBNAME NVARCHAR(128) = (SELECT DB_NAME());

	-- Decide to join existing transaction or start new 
	IF @TranCounter > 0 
		SAVE TRANSACTION @SavePoint; 
	ELSE 
		BEGIN TRANSACTION; 

	/* Borger kan ændres bagud i tid, således perioder ændres - følgende procedure tager sætter nye DW_SK_Borger på relevante steder */
	'+@SQLReadjustBorger+'
			
	;MERGE fct.'+@fctTableName+' WITH (TABLOCK) target
	USING #stg source 
	ON  source.'+@fctPKColumn+' = target.'+@fctPKColumn+'
	WHEN MATCHED --AND source.DW_IsDeleted = 0 
	THEN
	UPDATE
	SET  
		'+REPLACE(@fctColumnListeUpdate,',',CHAR(9)+CHAR(9)+',')+'
		,Target.[DW_ID_Audit_Update] = @DW_ID_Audit
	WHEN NOT MATCHED BY TARGET THEN
	INSERT	(
			'+REPLACE(@fctColumnList,',[',CHAR(9)+CHAR(9)+CHAR(9)+',[')+'
			,[DW_ID_Audit_Insert]
			,[DW_ID_Audit_Update])
	VALUES  (
			 '+REPLACE(REPLACE(@fctColumnList,'[','source.['),',',CHAR(9)+CHAR(9)+CHAR(9)+',')+'
			,@DW_ID_Audit
			,@DW_ID_Audit)
	OUTPUT $action into @rowcounts;

	
	SELECT	@RowsInserted =	ISNULL(SUM(CASE WHEN mergeAction = ''INSERT'' THEN 1 ELSE 0 END), 0),
			@RowsUpdated  =	ISNULL(SUM(CASE WHEN mergeAction = ''UPDATE'' THEN 1 ELSE 0 END), 0)
	FROM    @rowcounts  
	
	/* Fjern rækker fra fct der har DW_SK værdier der er SLETTET 
	DELETE fk
	FROM
		fct.Kontakt fk
	JOIN
		map.Kontakt_IFDB mk
		ON (fk.DW_SK_Kontakt = mk.DW_EK_Kontakt 
			AND mk.DW_IsDeleted = 1 
		   )
	
	SET @RowsDeleted = isnull(@@ROWCOUNT,0);
	*/
	 -- Update statistics for fact table 
 
	 UPDATE STATISTICS fct.'+@fctTableName+'; 
 
	 /* End updating fact table */ 
 
	 -- Commit only if the transaction was started in this procedure 
 
	 IF @TranCounter = 0 
		 COMMIT TRANSACTION; 
 
 END TRY 
 
 BEGIN CATCH 
 
	 -- Only rollback if transaction was started in this procedure 
	 IF @TranCounter = 0 
		ROLLBACK TRANSACTION; 
	 -- Its not our transaction but its still OK 
	 ELSE IF XACT_STATE() = 1 
		ROLLBACK TRANSACTION @SavePoint 
	 -- All hope is lot - rollback! 
	 ELSE IF XACT_STATE() = -1 
		ROLLBACK TRANSACTION; 
 
	 THROW; 
	 RETURN 1; 
 
 END CATCH'

SET @TotalPrints = (LEN(@sql) / 4000) + 1
SET @Counter=0
WHILE @Counter < @TotalPrints 
BEGIN
	PRINT(SUBSTRING(@sql,1+@Counter*4000,@Counter*4000+4000 ))
    SET @Counter = @Counter + 1
END




IF(@PrintOnly=0) 
BEGIN 
EXEC @dbExec @SQL
END





/*
Need to have
	-Validering
	-Mangler det noget standardkode?
	

	-Håndter at dw_sk_ allerede er fundet i stg.
	-Union ved flere kilder i SP fctUpdate
		-- tag hånd om special logik her (dette lave ssom logik tags)
	-delete(Delete udfra anden Nøgle end PK f.eks. bw_ek_borger og insert delta. f.eks SFI'erne)/fullload(trunc insert) versioner
	-handle deleted rows (DW_IS_DELETED kolonne i stg sp og stg table, dette gøres ved at opdatere map-tabellen)
	-Index optimering på staging tables

Nice to have
	Message: Husk Marker primarykey og NOT NULL
        Message: Kun Int, Decimal (MAX 18 ellers virker indexes ikke), varbinary, Nvarchar, Datetime2(3) understøttet
		Message: Mangler Komma
		Warning ved logik tags
		Warning ingen datoer til T2Dim angivet. Ignorer hvis ingen T2dim
	
	
	-Beautify
	-dokumenter
	-opskrift
	-Kun opdatering af tabeller (Mest til eksisterende kode)

*/


END
GO


