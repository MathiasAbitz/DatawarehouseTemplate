CREATE PROCEDURE [dbo].[AutoDimKodeGenerator]
@inputvar_DSOdatabase NVARCHAR(100),
@inputvar_kildeNavn NVARCHAR(100),
@dimTableName NVARCHAR(200),
@InputDimColumnDTypeList NVARCHAR(MAX),
--@dimMAPBKColumnList NVARCHAR(MAX),
@multitenanttype tinyint,
@PrintOnly bit =1
AS
BEGIN

/* HOW TO:
AutodimGenerator skal køres fra dwFramework.
@inputvar_DSOdatabase er databasen hvor dimensionen skal ende.
@InputDimColumnDTypeList angiver kolonner og datatyper og har samme syntax som indholdet i et create statement.
Kolonner som kan bruges lige nu: bigint, int, decimal (laves om til 18,2) , nvarchar (max virker også) ,datetime2
skal udvides med bit,smallint, tinyint, nchar, varbinary,date
Der skal være [] omkring datatyperne og kolonnenavnet ( dette scriptes ofte ud af create statementet). 
Angiv én primærnøgle i kolonnelisten med /*primarykey*/. Den kommer ikke med i dimensionen (Sharedimensions teknisk ting men det rammer alle dimensioner) og PK skal være 1 kolonne(Så concat er ofte nødvendigt), under 900 bytes og skal være NOT NULL. Brug evt. en hashbyte MD5sum som castes til en nvarchar(32). 
Hvis PK nøglen skal vises i dimensionen så lav en anden kolonne med et andet navn til det, men pas på udfaldet.
DW_SK, DW_EK og andre DW nøgler skal ikke specificeres medmindre man laver en outrigger. Se dim.organsiation med dim.Adress.
Efter hver kolonne skrives historiktypen i /**/. Det kan vælges mellem type1, type2 og type6. Ved type6 antages type 2 og den opretter selv nuværende kolonner
Hvis man vil bygge egen historik ( hvis man har til og fra datoer med historik i arc tabellerne) så sættes alle kolonner til /*type1*/, nøglen angives stadig som /*primarykey*/ og validfrom datoen markeres med /*ValidFrom*/. Der skal ikke angives validto
@printonly=1 printer alle statement til messages. 
@printonly=0 execute med det samme ( pas på med multitenancy og shareddimension.)
/*logik start*/ og /*logik slut*/ -tag i alle storedprocedures. Her kan man lave userdefined logik, som  ved en ny execute af AutoDimKodeGenerator overfører til det nye CREATE statement af storedprocedurene. 
Ved multitenancy og shareddimensions skal man selv rette viewet map.xxx til. Og man skal samle stg tabeller i storedproceduren dim.updateXXX
@multitenanttype=0 ingen multitenancy og brugerne deler rækker og kan se alles rækker. KundeID er ikke med i map tabels. 
@multitenanttype=1 
Når storedproceduren er kørt succesful, så put ETL-logik i SP stg.InsertdimXXX. Her finder du yderligere en cleanCode. Sættes som udgangspunkt altid = primærnøglen. Men bruges til når primærnøglen ikke er pæn, i det tilfælde er cleanCode er en pæn kode hvor vi kan mappe EK'ere på tværs af kilder.
Ved shareddimension, hvor alle berigende kolonner ligger andet steds. F.eks. dim.Organisation. Så skrives alle kolonner stadig i InputDimColumnDTypeList. Udfyld da kun de kolonner som er nødvendige i SP stg.DimXXX_kilde
3 situationer skriv om dem.
*/



/*WHAT DOES IT DO
Opretter sequence til dimensionen
Opretter og retter i stg,map og dim tabeller
Opretter map view 
Opretter og retter i storedprocedures til stg,map og dim, men gemme alt fra logik tags
*/
/*Sørger for at vi kan execute AUTODimGenerator på den rigtige server*/
DECLARE @dbExec NVARCHAR(100)
SET @dbExec=@inputvar_DSOdatabase+N'.sys.sp_executesql'
DECLARE @stgFilegroup nvarchar(200)
DECLARE	@modelFilegroup NVARCHAR(200)
/*de filgroup der bruges som standard. Disse er hardcodet og faster her*/
SET @stgFilegroup='Staging'
SET @modelFilegroup='Standard'
Declare @Original_InputDimColumnDTypeList nvarchar(max)
set @Original_InputDimColumnDTypeList=@InputDimColumnDTypeList

/*Bruges til at printe for lange messages*/
DECLARE @Counter INT
SET @Counter = 0
DECLARE @TotalPrints INT

/*Inkludere altid nuværendekolonner*/
DECLARE @IncludeNuvCol INT
SET @IncludeNuvCol= 1

DECLARE @SQL NVARCHAR(MAX)
DECLARE @table_name NVARCHAR(512)
DECLARE @NL char(2)
SET @NL=CHAR(13)+CHAR(10)

/*Supported datatypes*/
declare @tabledatatype TABLE
(
	Datatype nvarchar(20),
	UnknownValue nvarchar(200)
)
insert into @tabledatatype (Datatype,UnknownValue)
values 
	('[BIT]','CAST(0 as bit)'),
	('[NVARCHAR]','N''Ukendt'''),
	('[nchar]','N''~U'''),
	('[Date]','CAST(N''0001-01-01'' AS DATETIME2(3))'),
	('[Datetime2]','CAST(N''0001-01-01'' AS DATE)'),
	('[BIGINT]','CAST(-1 as BIGINT)'),
	('[INT]','CAST(-1 as INT)'),
	('[SMALLINT]','CAST(-1 as SMALLINT)'),
	('[TINYINT]','CAST(0 as TINYINT)'),
	('[DECIMAL]','''-1'''),
	('[varbinary]','CAST(''Ukendt'' as varbinary(32))')


/*Går i gang med at kigge på decimalerne. De driller pga , i f.eks. (18,2)*/
DECLARE @pos INT
DECLARE @Oldpos INT
DECLARE @subs NVARCHAR(max)
SET @pos=0
SET @oldpos=0

IF @InputDimColumnDTypeList like '%[DECIMAL](%'
BEGIN
	WHILE @pos >= @oldpos
		begin
		
	/*Finder næste decimal*/		
			SET @subs= SUBSTRING(@InputDimColumnDTypeList,CHARINDEX('[DECIMAL](',@InputDimColumnDTypeList,@pos),CHARINDEX(')',@InputDimColumnDTypeList,CHARINDEX('[DECIMAL](',@InputDimColumnDTypeList,@pos)) -CHARINDEX('[DECIMAL](',@InputDimColumnDTypeList,@pos)+1 )
		
	/*Replace alle occurences med samme decimal presicion*/
	
			SET @InputDimColumnDTypeList= REPLACE(@InputDimColumnDTypeList,@subs, Replace(@subs,',',';'))
		
		/*Sætter postition på nuværende decimal, så vi kan søge frem efter den næste decimal*/	
			SET @Oldpos=CHARINDEX('[DECIMAL](',@InputDimColumnDTypeList,@pos)+CHARINDEX(')',@InputDimColumnDTypeList,CHARINDEX('[DECIMAL](',@InputDimColumnDTypeList,@pos)) -CHARINDEX('[DECIMAL](',@InputDimColumnDTypeList,@pos)
			SET @pos =CHARINDEX('[DECIMAL](',@InputDimColumnDTypeList,@Oldpos)


		END
END		

/*Laver list med kolonner til dimensionen med datatyper */
DECLARE @dimColumnDTypeList NVARCHAR(MAX)
SELECT @dimColumnDTypeList= COALESCE(@dimColumnDTypeList+','+@NL, '') +REPLACE(l.value,@NL,'')
FROM  String_Split(@InputDimColumnDTypeList,',') l


/*laver en lidt med alle kolonner inkl nuværende kolonner*/
DECLARE @dimNuvColumnDTypeList NVARCHAR(MAX)
SELECT @dimNuvColumnDTypeList= COALESCE(@dimNuvColumnDTypeList+','+@NL, '') +REPLACE(IIF(c.col IS NOT NULL,c.col, l.value),@NL,'')
FROM  String_Split(@InputDimColumnDTypeList,',') l
OUTER APPLY (SELECT Col,1 AS  sort FROM (	SELECT l.Value AS Col
								UNION ALL
								SELECT REPLACE(REPLACE(l.value,SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)),REPLACE(SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)),'[','[NUV_')),'NOT NULL','NULL') AS Col

						) AS c WHERE l.value LIKE '%type6%'

					) AS c
where value not like '%/*PrimaryKey*/%'					

/*laver en liste med kolonner til dimensionen uder datatyper*/
Declare  @dimColumnList  NVARCHAR(MAX)
SELECT  @dimColumnList = COALESCE(@dimColumnList + ',', '') + SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) +@NL
FROM  String_Split(@dimColumnDTypeList,',')
where value not like '%/*PrimaryKey*/%'	

/*laver list til stg tabel med datatyper, laves seperart ford ivi tilføjer til den senere afhængigt af multitenancy*/
Declare  @stgdimColumnList  NVARCHAR(MAX)
SELECT  @stgdimColumnList = COALESCE(@stgdimColumnList + ',', '') + SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) +@NL
FROM  String_Split(@dimColumnDTypeList,',')

/*laver coalesce til dim.updatexxx. Her kan vi måske gøre noget smartere med REPLACERNE deruer i forhold til default value*/
Declare  @dimColumnListCoalesce  NVARCHAR(MAX)
SELECT  @dimColumnListCoalesce = COALESCE(@dimColumnListCoalesce + ',', '') + 'Coalesce('+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+','+tdt.UnknownValue+')' +@NL
FROM  String_Split(@dimColumnDTypeList,',') l
join @tabledatatype as tdt on tdt.Datatype=SUBSTRING(l.value, CHARINDEX('[',l.value,(charindex('[',l.value)+1)), CHARINDEX(']',l.value,(charindex(']',l.value)+1))-CHARINDEX('[',l.value,(charindex('[',l.value)+1))+1)
where value not like '%/*PrimaryKey*/%'	

/*selecter valid from kolonnen*/
Declare  @dimValidFromColumn  NVARCHAR(MAX)
SELECT @dimValidFromColumn= SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))
FROM  String_Split(@dimColumnDTypeList,',')
WHERE value LIKE '%/*ValidFrom*/%'

Declare  @dimValidToColumn  NVARCHAR(MAX)
SELECT @dimValidToColumn= SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))
FROM  String_Split(@dimColumnDTypeList,',')
WHERE value LIKE '%/*ValidTo*/%'

print(@dimValidToColumn)
/*Liste med TYPE1 kolonner med datatyper*/
Declare  @dimColumnListT1  NVARCHAR(MAX)
SELECT @dimColumnListT1=
COALESCE(@dimColumnListT1 + ',', '') + SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) +@NL
FROM  String_Split(@dimColumnDTypeList,',')
WHERE value LIKE '%/*type1*/%'

/*Primærnøglen til map tabellen uden datatatype*/
DECLARE @dimMAPBKColumnList NVARCHAR(MAX)
SELECT @dimMAPBKColumnList=
COALESCE(@dimMAPBKColumnList + ',', '') + SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) +@NL
FROM  String_Split(@dimColumnDTypeList,',')
WHERE value LIKE '%/*PrimaryKey*/%'

/*bygger joinet til sp map.UpdateXXX*/
DECLARE @dimMapBKJoin NVARCHAR(MAX);
Select @dimMapBKJoin = COALESCE(@dimMapBKJoin + ' AND ', '') + 'target.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+'= source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) +@NL
FROM  String_Split(@dimColumnDTypeList,',') AS l
WHERE l.value LIKE '%/*PrimaryKey*/%'

/*Primærnøglen til map tabellen med datatatype*/
DECLARE @dimMapColumnListDtype NVARCHAR(MAX)
SELECT @dimMapColumnListDtype = COALESCE(@dimMapColumnListDtype + ',', '')+value 
FROM String_Split(@dimColumnDTypeList,',') AS l
WHERE l.value LIKE '%/*PrimaryKey*/%'

/*LAver Ukendt list til dim.UpdateXXX. Her kan vi måske gøre noget smartere med REPLACERNE i forhold til default value*/
DECLARE @dimUkendtListe NVARCHAR(MAX);
Select @dimUkendtListe = COALESCE(@dimUkendtListe + ',', '') +tdt.UnknownValue  +@NL
FROM  String_Split(@dimNuvColumnDTypeList,',') AS l
join @tabledatatype as tdt on tdt.Datatype=SUBSTRING(l.value, CHARINDEX('[',l.value,(charindex('[',l.value)+1)), CHARINDEX(']',l.value,(charindex(']',l.value)+1))-CHARINDEX('[',l.value,(charindex('[',l.value)+1))+1)
where value not like '%/*PrimaryKey*/%'	

/*Laver update liste til dim.UpdateXXX. Her kan vi måske gøre noget smartere med REPLACERNE i forhold til default value*/
DECLARE @dimColumnListeUpdate NVARCHAR(MAX);
Select @dimColumnListeUpdate = COALESCE(@dimColumnListeUpdate + ',', '') + SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) +'= Coalesce([source].'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+','+tdt.UnknownValue+')' +@NL
FROM  String_Split(@dimColumnDTypeList,',') AS l
join @tabledatatype as tdt on tdt.Datatype=SUBSTRING(l.value, CHARINDEX('[',l.value,(charindex('[',l.value)+1)), CHARINDEX(']',l.value,(charindex(']',l.value)+1))-CHARINDEX('[',l.value,(charindex('[',l.value)+1))+1)
where value not like '%/*PrimaryKey*/%'	

/*Laver update liste til dim.UpdateXXX. Her kan vi måske gøre noget smartere med REPLACERNE i forhold til default value*/
DECLARE @dimColumnListeUpdateT1 NVARCHAR(MAX);
Select @dimColumnListeUpdateT1 = COALESCE(@dimColumnListeUpdateT1 + ',', '') + SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) +'= Coalesce([source].'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+','+tdt.UnknownValue+')' +@NL
FROM  String_Split(@dimColumnDTypeList,',') AS l
join @tabledatatype as tdt on tdt.Datatype=SUBSTRING(l.value, CHARINDEX('[',l.value,(charindex('[',l.value)+1)), CHARINDEX(']',l.value,(charindex(']',l.value)+1))-CHARINDEX('[',l.value,(charindex('[',l.value)+1))+1)
WHERE l.value LIKE '%/*type1*/%'

/*Laver compare af alle kolonner til dim.updateXXX. Bruges når man har egen validFrom med*/
Declare  @dimColumnListComp  NVARCHAR(MAX)
SELECT  @dimColumnListComp = 
COALESCE(@dimColumnListComp + ' OR '+@NL, '')+'((target.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' IS NULL AND Source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' IS NOT NULL) OR (Target.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' != source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' AND Source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' IS NOT NULL ))'
FROM  String_Split(@dimColumnDTypeList,',')
where value not like '%/*PrimaryKey*/%'	

/*Laver compare af alle type1 kolonner til dim.updateXXX.*/
Declare  @dimColumnListCompT1  NVARCHAR(MAX)
SELECT  @dimColumnListCompT1 = 
COALESCE(@dimColumnListCompT1 + ' OR '+@NL, '')+'((target.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' IS NULL AND Source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' IS NOT NULL) OR (Target.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' != source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' AND Source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' IS NOT NULL ))'
FROM  String_Split(@dimColumnDTypeList,',')	l
WHERE l.value LIKE '%/*type1*/%'

/*Laver compare af alle type2 eller tpye6 kolonner til dim.updateXXX.*/
Declare  @dimColumnListCompT2  NVARCHAR(MAX)
SELECT  @dimColumnListCompT2 = 
COALESCE(@dimColumnListCompT2 + ' OR '+@NL, '')+'((target.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' IS NULL AND Source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' IS NOT NULL) OR (Target.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' != source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' AND Source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' IS NOT NULL ))'
FROM  String_Split(@dimColumnDTypeList,',')	
WHERE value LIKE '%/*type2*/%' or value LIKE '%/*type6*/%'

/*AFTERBURN: Kode til at angiver hvilke kolonner som gav anledning til type2 eller type6 ændring. Bruges i dim.updateXXX.*/
Declare  @dimColumnListChgRT2  NVARCHAR(MAX)
SELECT  @dimColumnListChgRT2 = 
COALESCE(@dimColumnListChgRT2 + ' + '+@NL, '')+'IIF(target.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+' = Source.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+','''+SUBSTRING( value,CHARINDEX('[',value,1)+1,-1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+'#'','''')' 
FROM  String_Split(@dimColumnDTypeList,',')	
WHERE value LIKE '%/*type2*/%' or value LIKE '%/*type6*/%'

/*AFTERBURN: Kode til at opdaterer alle nuværende kolonner ved type6 ændringer. Bruges i dim.updateXXX.*/
Declare  @dimColumnListNuv  NVARCHAR(MAX)
SELECT  @dimColumnListNuv = 
COALESCE(@dimColumnListNuv + @NL, '')+',target.[NUV_'+SUBSTRING( value,CHARINDEX('[',value,1)+1,-1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1))+'] = nuv.'+SUBSTRING( value,CHARINDEX('[',value,1),1+CHARINDEX(']',value,1)-CHARINDEX('[',value,1)) 
FROM  String_Split(@dimColumnDTypeList,',')	
WHERE value LIKE '%/*type6*/%'

/*Variable som bruges ved multitenancy*/
DECLARE @stgdimColumnDTypeList nvarchar(max)
Declare @multiTMAPBKColumnList nvarchar(max)
DECLARE @multiTdimMapBKJoin nvarchar(max)
DECLARE @kundeID nvarchar(max)
DECLARE @multiTdimMapBK nvarchar(max)


--MultiTenancy 
IF @multitenanttype=0
	BEGIN
		/*Ingen multiteneacy. DW_ID_Kunde bruges ikke*/
		SET @multiTMAPBKColumnList=@dimMAPBKColumnList
		set @stgdimColumnDTypeList=@dimColumnDTypeList
		set @multiTdimMapBKJoin=@dimMapBKJoin
		set @kundeID=''
		/*Compare fra en map.XXX_kilde til viewet map.XXX går altid gennem CleanCode*/
		set @multiTdimMapBK='target.[CleanCode]=source.[CleanCode]'
		PRINT('MultiTenanttype0')
	END
ELSE
	BEGIN
		IF @multitenanttype =1 OR @multitenanttype=2
		BEGIN
			IF @multitenanttype =1
			BEGIN
				PRINT('MultiTenanttype1')
			/*tilføjer dw_ID_Kunde i map tabellen. Tilføjes ikke til @multiTdimMapBK for at dele DW_EK på tværes af kunder */
			SET @dimMapColumnListDtype=@dimMapColumnListDtype+N','+@NL+N'[DW_ID_Kunde] INT NOT NULL'
			SET @multiTMAPBKColumnList=@dimMAPBKColumnList+N',[DW_ID_Kunde]'
			SET @stgdimColumnDTypeList= @dimColumnDTypeList+@NL+N',[DW_ID_Kunde] INT NOT NULL'
			SET @stgdimColumnList= @stgdimColumnList+@NL+N',[DW_ID_Kunde]'
			/*bruges mod egen map table, fordi egen map table kan indeholde flere kunder*/
			set @multiTdimMapBKJoin=@dimMapBKJoin+N'AND target.[DW_ID_Kunde]=source.[DW_ID_Kunde]'
			/*Bruges mod fælles map table*/
			set @multiTdimMapBK='target.[CleanCode]=source.[CleanCode]'
			set @kundeID=''
			END
			IF @multitenanttype =2
			BEGIN
				PRINT('MultiTenanttype2')
			/*tilføjer dw_ID_Kunde så DW_EK også er afhængig af DW_ID_kunde*/
			SET @dimMapColumnListDtype=@dimMapColumnListDtype+N','+@NL+N'[DW_ID_Kunde] INT NOT NULL'
			SET @multiTMAPBKColumnList=@dimMAPBKColumnList+N',[DW_ID_Kunde]'
			SET @stgdimColumnDTypeList= @dimColumnDTypeList+@NL+N',[DW_ID_Kunde] INT NOT NULL'
			SET @stgdimColumnList= @stgdimColumnList+@NL+N',[DW_ID_Kunde]'
			set @multiTdimMapBKJoin=@dimMapBKJoin+N'AND target.[DW_ID_Kunde]=source.[DW_ID_Kunde]'
			set @dimMAPBKColumnList=@dimMAPBKColumnList+ N',[DW_ID_Kunde]'
			set @dimMapBKJoin=@dimMapBKJoin+N'AND target.[DW_ID_Kunde]=source.[DW_ID_Kunde]'
			set @kundeID='[DW_ID_Kunde],'
			set @multiTdimMapBK='target.[CleanCode]=source.[CleanCode] AND target.[DW_ID_Kunde]=source.[DW_ID_Kunde]'
			END
		END
		ELSE
		BEGIN
		
			RAISERROR ('Fejl i multitenanttype ', 11,1)
		END
	END




                      
/*****************************SEQUENCES*****************************************/

SET @SQL='
IF (OBJECT_ID ('''+@inputvar_DSOdatabase+'.dim.EK_'+@dimTableName+''') IS NULL) '+@NL+
'Begin
/****** Object:  Sequence [dim].[EK_'+@dimTableName+']    Script Date: '+CAST(GETDATE() AS NVARCHAR(30))+' ******/'+@NL+'
CREATE SEQUENCE [dim].[EK_'+@dimTableName+'] 
 AS [int]
 START WITH 1
 INCREMENT BY 1
 MINVALUE -2147483648
 MAXVALUE 2147483647
 CACHE  1000 


ALTER AUTHORIZATION ON [dim].[EK_'+@dimTableName+'] TO  SCHEMA OWNER 
END
'
PRINT(@SQL)
IF(@PrintOnly=0) 
BEGIN 
EXEC @dbExec @SQL
END
/*****************************TABLES********************************************/

/*Oprettet ChangedColumns i dimensionen hvis derskal være historik*/
Declare @changeCol nvarchar(100)
IF @dimColumnListChgRT2 IS NOT NULL 
	BEGIN 
		set @changeCol= '[ChangedColumns] nvarchar(2000) NOT NULL,'
	END
else 
	BEGIN
		set @changeCol=''
	END

/*Dimension*/
SET @sql =
N'IF (OBJECT_ID ('''+@inputvar_DSOdatabase+'.dim.'+@dimTableName+''') IS NOT NULL) drop table dim.'+@dimTableName+''+@NL+
'
/****** Object:  Table [dim].['+@dimTableName+']    Script Date: '+CAST(GETDATE() AS NVARCHAR(30))+' ******/'+@NL+'
SET ANSI_NULLS ON


SET QUOTED_IDENTIFIER ON


CREATE TABLE [dim].['+@dimTableName+'](
	[DW_SK_'+@dimTableName+'] [int] NOT NULL IDENTITY(1,1),
	[DW_EK_'+@dimTableName+'] [int] NOT NULL,
	[CleanCode] nvarchar(200) NOT NULL,
	'+REPLACE(@dimNuvColumnDTypeList,';',',')+',
	'+@changeCol+'
	[DW_ValidFrom] [datetime2](2) NOT NULL,
	[DW_ValidTo] [datetime2](2) NOT NULL,
	[DW_IsInferred] [bit] NOT NULL,
	[DW_IsCurrent] [bit] NOT NULL,
	[DW_IsDeleted] [bit] NOT NULL,
	[DW_ID_Audit_Insert] [int] NOT NULL,
	[DW_ID_Audit_Update] [int] NOT NULL,
 CONSTRAINT [PK_'+@dimTableName+'] PRIMARY KEY CLUSTERED 
(
	[DW_SK_'+@dimTableName+'] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON ['+@modelFilegroup+'],
 CONSTRAINT [UQ_'+@dimTableName+'_EK] UNIQUE NONCLUSTERED 
(
	[DW_EK_'+@dimTableName+'] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, DATA_COMPRESSION = ROW) ON ['+@modelFilegroup+']
) ON ['+@modelFilegroup+']


ALTER AUTHORIZATION ON [dim].['+@dimTableName+'] TO  SCHEMA OWNER 

ALTER TABLE [dim].['+@dimTableName+'] ADD  CONSTRAINT [DF_D'+@dimTableName+'_AuditInsert]  DEFAULT ((0)) FOR [DW_ID_Audit_Insert]
ALTER TABLE [dim].['+@dimTableName+'] ADD  CONSTRAINT [DF_D'+@dimTableName+'_AuditUpdate]  DEFAULT ((0)) FOR [DW_ID_Audit_Update]
ALTER TABLE [dim].['+@dimTableName+'] ADD  CONSTRAINT [DF_D'+@dimTableName+'_ValidFrom]  DEFAULT (''00010101'') FOR [DW_ValidFrom]
ALTER TABLE [dim].['+@dimTableName+'] ADD  CONSTRAINT [DF_D'+@dimTableName+'_ValidTo]  DEFAULT (''99991231'') FOR [DW_ValidTo]
ALTER TABLE [dim].['+@dimTableName+'] ADD  CONSTRAINT [DF_D'+@dimTableName+'_IsInferred]  DEFAULT ((0)) FOR [DW_IsInferred]
ALTER TABLE [dim].['+@dimTableName+'] ADD  CONSTRAINT [DF_D'+@dimTableName+'_IsCurrent]  DEFAULT ((1)) FOR [DW_IsCurrent]
ALTER TABLE [dim].['+@dimTableName+'] ADD  CONSTRAINT [DF_D'+@dimTableName+'_IsDeleted]  DEFAULT ((0)) FOR [DW_IsDeleted]
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


/*Map*/
SET @sql =
N'IF (OBJECT_ID ('''+@inputvar_DSOdatabase+'.map.'+@dimTableName+'_'+@inputvar_kildeNavn+''') IS NOT NULL) drop table map.'+@dimTableName+'_'+@inputvar_kildeNavn+''+@NL+
'
/****** Object:  Table [map].['+@dimTableName+'_'+@inputvar_kildeNavn+']    Script Date: '+CAST(GETDATE() AS NVARCHAR(30))+' ******/'+CHAR(10)+'
SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

SET ANSI_PADDING ON

CREATE TABLE [map].['+@dimTableName+'_'+@inputvar_kildeNavn+'](
	[DW_EK_'+@dimTableName+'] [INT] NOT NULL,
	'
	+@dimMapColumnListDtype+','+
	'
	[CleanCode] NVARCHAR(200) NOT NULL, 
	[DW_ValidFrom] [DATETIME2](2) NOT NULL,
	[DW_ValidTo] [DATETIME2](2) NOT NULL,
	[DW_ID_Audit_Insert] [INT] NOT NULL,
	[DW_ID_Audit_Update] [INT] NOT NULL,
	[DW_IsDeleted] [TINYINT] NOT NULL,
 CONSTRAINT [PK_'++@dimTableName+'_'+@inputvar_kildeNavn+'] PRIMARY KEY NONCLUSTERED 
(
'+REPLACE(@multiTMAPBKColumnList,',',' ASC,'+@NL)+' ASC'+
'
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON ['+@modelFilegroup+']
) ON ['+@modelFilegroup+']

SET ANSI_PADDING ON

ALTER AUTHORIZATION ON [map].['+@dimTableName+'_'+@inputvar_kildeNavn+'] TO  SCHEMA OWNER 

ALTER TABLE [map].['+@dimTableName+'_'+@inputvar_kildeNavn+'] ADD  CONSTRAINT [DF_'+@dimTableName+'_'+@inputvar_kildeNavn+'_EK]  DEFAULT (NEXT VALUE FOR [dim].[EK_'+@dimTableName+']) FOR [DW_EK_'+@dimTableName+']
ALTER TABLE [map].['+@dimTableName+'_'+@inputvar_kildeNavn+'] ADD  CONSTRAINT [DF_'+@dimTableName+'_'+@inputvar_kildeNavn+'_ValidFrom]  DEFAULT (''00010101'') FOR [DW_ValidFrom]
ALTER TABLE [map].['+@dimTableName+'_'+@inputvar_kildeNavn+'] ADD  CONSTRAINT [DF_'+@dimTableName+'_'+@inputvar_kildeNavn+'_ValidTo]  DEFAULT (''99991231'') FOR [DW_ValidTo]
ALTER TABLE [map].['+@dimTableName+'_'+@inputvar_kildeNavn+'] ADD  CONSTRAINT [DF_'+@dimTableName+'_'+@inputvar_kildeNavn+'_IsDeleted]  DEFAULT (''0'') FOR [DW_IsDeleted]
'

SET @TotalPrints = (LEN(@sql) / 4000) + 1
SET @Counter=0
WHILE @Counter < @TotalPrints 
BEGIN
	PRINT(SUBSTRING(@sql,1+@Counter*4000,@Counter*4000+4000 ))
    SET @Counter = @Counter + 1
END


IF(@printonly=0) 
BEGIN 
EXEC @dbExec @SQL
END



/*Stg*/
SET @sql =
N'IF (OBJECT_ID ('''+@inputvar_DSOdatabase+'.stg.dim'+@dimTableName+'_'+@inputvar_kildeNavn+''') IS NOT NULL) drop table stg.dim'+@dimTableName+'_'+@inputvar_kildeNavn+''+@NL+
'
/****** Object:  Table [stg].[dim'+@dimTableName+'_'+@inputvar_kildeNavn+']    Script Date: '+CAST(GETDATE() AS NVARCHAR(30))+' ******/'+CHAR(10)+'
SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

SET ANSI_PADDING ON

CREATE TABLE [stg].[dim'+@dimTableName+'_'+@inputvar_kildeNavn+'](
	'
	+REPLACE(@stgdimColumnDTypeList,';',',')+',
	[CleanCode] NVARCHAR(200) NOT NULL
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


IF(@printonly=0) 
BEGIN 
EXEC @dbExec @SQL
END


/*****************************Views********************************************/

/*Map*/
/*Ret til så koden tager det gamle indhold og tilføjer nyt ved tilføjelse af ny kilde*/
IF (OBJECT_ID (''+@inputvar_DSOdatabase+'.map.'+@dimTableName) IS NULL)
BEGIN
SET @SQL='
CREATE VIEW [map].['+@dimTableName+']
AS
SELECT 
	[mapALLE].[DW_EK_'+@dimTableName+'],
	[CleanCode],
	'+@kundeID+'
	DW_ID_Audit_Update,
	CAST(IIF(map'+@inputvar_kildeNavn+'.DW_EK_'+@dimTableName+' IS NULL, 0, 1) AS TINYINT) AS Medi'+@inputvar_kildeNavn+'
FROM (
	SELECT 
		mapUpdate.[DW_EK_'+@dimTableName+'], 
		mapUpdate.[CleanCode], 
		'+@kundeID+'
		MAX(DW_ID_Audit_Update) AS DW_ID_Audit_Update
	FROM (	
		SELECT DISTINCT 
			DW_EK_'+@dimTableName+',
			'+@kundeID+' 
			[CleanCode],
			DW_ID_Audit_Update
				
		FROM map.'+@dimTableName+'_'+@inputvar_kildeNavn+'
		/*
		UNION
		SELECT DISTINCT DW_EK_xxx,
				<metode til at udlede shak fra andet kildesystem>  AS,
				KodeType
		FROM map.xxx_kilde
		*/
	) AS mapUpdate
	group by mapUpdate.[DW_EK_'+@dimTableName+'],'+@kundeID+'[CleanCode]
) mapALLE
LEFT JOIN (SELECT DISTINCT DW_EK_'+@dimTableName+' FROM map.'+@dimTableName+'_'+@inputvar_kildeNavn+') AS map'+@inputvar_kildeNavn+' 
ON map'+@inputvar_kildeNavn+'.DW_EK_'+@dimTableName+' = mapALLE.DW_EK_'+@dimTableName+'
'
SET @TotalPrints = (LEN(@sql) / 4000) + 1
SET @Counter=0
WHILE @Counter < @TotalPrints 
BEGIN
	PRINT(SUBSTRING(@sql,1+@Counter*4000,@Counter*4000+4000 ))
    SET @Counter = @Counter + 1
END

EXEC @dbExec @SQL
END
ELSE
	Begin
		PRINT('Fix map view yourself, lazy bastard')
	END
/*****************************StoredProcedure********************************************/
/*Staging*/
	
DECLARE @logik nvarchar(MAX)
/*defult logik*/
SET @logik= 'CREATE TABLE #TEMP
	(
'+REPLACE(@stgdimColumnDTypeList,';',',')+'
	)'
/*Tjekker for eksisterende SP*/
IF(OBJECT_ID(N''+@inputvar_DSOdatabase+'.stg.Selectdim'+@dimTableName+'_'+@inputvar_kildeNavn) IS NOT NULL)
	BEGIN
		/*Finder definitionen af eksisterende SP*/
		DECLARE @def NVARCHAR(MAX)
			Set @SQL = N'SELECT @def= OBJECT_DEFINITION( OBJECT_ID(N'''+@inputvar_DSOdatabase+'.stg.Selectdim'+@dimTableName+'_'+@inputvar_kildeNavn+'''))'
		exec @dbExec @SQL,N'@def nvarchar(max) out', @def out
	
	/*Klipper logik-tags ud af eksisterende SP. Der kan kun være 1 logiktag i stg.insertXXX_Kilde */
		SELECT @logik=SUBSTRING(@def
			,LEN('/*LOGIK START*/')+CHARINDEX('/*LOGIK START*/',@def)
			,-(1+LEN('/*LOGIK SLUT*/'))+CHARINDEX('/*LOGIK SLUT*/',@def)-CHARINDEX('/*LOGIK START*/',@def)
			)


	END 

/*Starter på opbygning af stg SP*/
SET @SQL=
'
/****** Object:  StoredProcedure [stg].[SelectDim'+@dimTableName+'_'+@inputvar_kildeNavn+']    Script Date: 22-12-2017 10:16:35 ******/
'+IIF(OBJECT_ID (''+@inputvar_DSOdatabase+'.stg.SelectDim'+@dimTableName+'_'+@inputvar_kildeNavn) IS NULL,'CREATE','ALTER') +' PROCEDURE [stg].[SelectDim'+@dimTableName+'_'+@inputvar_kildeNavn+']
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


	TRUNCATE TABLE [stg].[Dim'+@dimTableName+'_'+@inputvar_kildeNavn+'];

	INSERT INTO stg.dim'+@dimTableName+'_'+@inputvar_kildeNavn+' (
	'+@stgdimColumnList+',
	[CleanCode]
	)
	SELECT 
'+@stgdimColumnList+'
,[CleanCode]
	from #temp as t


	UPDATE STATISTICS [stg].[Dim'+@dimTableName+'_'+@inputvar_kildeNavn+'];

/*last run with Auto:

	DECLARE	@return_value int

EXEC	@return_value = [dbo].[AutoDimKodeGenerator]
		@inputvar_DSOdatabase = N'''+@inputvar_DSOdatabase+''',
		@inputvar_kildeNavn = N'''+@inputvar_kildeNavn+''',
		@DimTableName = N'''+@dimTableName+''',
		@InputDimColumnDTypeList = 
N'''+@Original_InputDimColumnDTypeList+
''',
		@multitenanttype ='+cast(@multitenanttype as nvarchar(1))+',
		@PrintOnly = '+cast(@PrintOnly as nvarchar(1))+'

SELECT	''Return Value'' = @return_value
	
*/
'

/*Kan kun printe 4000 karaktere afgangen*/
SET @TotalPrints = (LEN(@sql) / 4000) + 1
SET @Counter=0
WHILE @Counter < @TotalPrints 
BEGIN
	PRINT(SUBSTRING(@sql,1+@Counter*4000,@Counter*4000+4000 ))
    SET @Counter = @Counter + 1
END


IF(@printonly=0) 
BEGIN 
EXEC @dbExec @SQL
END


/*Map*/
SET @def=''
SET @logik=''
/*Tjekker for eksisterende SP*/
IF(OBJECT_ID(N''+@inputvar_DSOdatabase+'.map.Update'+@dimTableName+'_'+@inputvar_kildeNavn) IS NOT NULL)
	BEGIN
		
		/*Selecter definition af eksisterende SP*/
			Set @SQL = N'SELECT @def= OBJECT_DEFINITION( OBJECT_ID(N'''+@inputvar_DSOdatabase+'.map.Update'+@dimTableName+'_'+@inputvar_kildeNavn+'''))'
			exec @dbExec @SQL,N'@def nvarchar(max) out', @def out
	
		/*Klipper logik-tags ud af eksisterende SP. Der kan kun være 1 logiktag i map.updateXXX_kilde */
		SELECT @logik=SUBSTRING(@def
			,LEN('/*LOGIK START*/')+CHARINDEX('/*LOGIK START*/',@def)
			,-(1+LEN('/*LOGIK SLUT*/'))+CHARINDEX('/*LOGIK SLUT*/',@def)-CHARINDEX('/*LOGIK START*/',@def)
			)

	END 

/*start på map SP opbygning.
Håndtere ikke tilfælde hvor en Primærnøgle ændrer EK. Søg inspiration i map.UpdateOrgnisation_prosang fra RM*/
SET @SQL=
'
'+IIF(OBJECT_ID (''+@inputvar_DSOdatabase+'.map.Update'+@dimTableName+'_'+@inputvar_kildeNavn) IS NULL,'CREATE','ALTER')+' PROCEDURE [map].[Update'+@dimTableName+'_'+@inputvar_kildeNavn+'] 
@DW_ID_Audit_EndValue_Last INT = NULL
,@DW_ID_Audit INT = NULL
AS
	BEGIN TRY

		DECLARE @TranCounter INT = @@TRANCOUNT, @SavePoint NVARCHAR(32) = CAST(@@PROCID AS NVARCHAR(20)) + N''_'' + CAST(@@NESTLEVEL AS NVARCHAR(2));

		IF @TranCounter > 0
			SAVE TRANSACTION @SavePoint
		ELSE
			BEGIN TRANSACTION

/*LOGIK START*/'
+@logik+
'/*LOGIK SLUT*/


		/* Allerede kendte entiteter fra andre map tabeller indsættes */
		;MERGE [map].['+@dimTableName+'_'+@inputvar_kildeNavn+'] as target
		USING (
				SELECT 
					target.DW_EK_'+@dimTableName+', '+REPLACE(@multiTMAPBKColumnList,'[','source.[')+',source.[CleanCode]
				FROM 
					(SELECT DISTINCT '+@multiTMAPBKColumnList+'
						,[CleanCode] 
					 FROM 
						[stg].[Dim'+@dimTableName+'_'+@inputvar_kildeNavn+']
					) source
				LEFT JOIN
					[map].'+@dimTableName+' target
					ON ('+@multiTdimMapBK+'
					)
				WHERE target.DW_EK_'+@dimTableName+' IS NOT NULL
		) as source
		ON ('+@multiTdimMapBKJoin+')
		WHEN MATCHED THEN
		UPDATE SET target.DW_ID_Audit_Update = @DW_ID_Audit
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (DW_EK_'+@dimTableName+', '+@multiTMAPBKColumnList+',[CleanCode],[DW_ID_Audit_Insert],[DW_ID_Audit_Update])
			VALUES (source.DW_EK_'+@dimTableName+', '+REPLACE(@multiTMAPBKColumnList,'[','source.[')+',[CleanCode],@DW_ID_Audit,@DW_ID_Audit)
		;

		/*	Da to forskellige NK kan mappe til samme BK, så laver vi her
			et trick der indsætter den den første NK for en given (ny) BK kode i
			map tabellen - således at der kommer en DW_EK nøgle på tabellen.
			I næste skridt indsætter vi så alle andre NK der matcher den nye BK
			kode - således alle NK for den samme BK kode får samme DW_EK nøgle
		*/
		;MERGE [map].['+@dimTableName+'_'+@inputvar_kildeNavn+'] AS target
		USING (
				SELECT target.DW_EK_'+@dimTableName+', '+REPLACE(@multiTMAPBKColumnList,'[','source.[')+',source.[CleanCode]
				FROM (
					SELECT 
						'+REPLACE(@multiTMAPBKColumnList,'[','source.[')+',source.[CleanCode]
					FROM 
						(SELECT '+@multiTMAPBKColumnList+',[CleanCode] , ROW_NUMBER() OVER (PARTITION BY '+@kundeid+'[CleanCode] ORDER BY '+COALESCE(@dimValidFromColumn,@dimMAPBKColumnList)+') AS RN
					 FROM 
						[stg].[Dim'+@dimTableName+'_'+@inputvar_kildeNavn+']
						) source
					WHERE RN = 1
				) source
				LEFT JOIN
					[map].'+@dimTableName+' target
					ON ('+@multiTdimMapBK+')
				WHERE target.DW_EK_'+@dimTableName+' IS NULL
		) AS source
		ON ('+@multiTdimMapBKJoin+')
		WHEN NOT MATCHED BY TARGET THEN
			INSERT ('+@multiTMAPBKColumnList+',[CleanCode],[DW_ID_Audit_Insert],[DW_ID_Audit_Update])
			VALUES ('+REPLACE(@multiTMAPBKColumnList,'[','source.[')+',[CleanCode],@DW_ID_Audit,@DW_ID_Audit)
		;

		/*	Helt nye entiteter der heller ikke eksisterer i 
			andre map tabeller indsættes 
		*/
		;MERGE [map].['+@dimTableName+'_'+@inputvar_kildeNavn+'] AS target
		USING (
				SELECT 
					target.DW_EK_'+@dimTableName+','+REPLACE(@multiTMAPBKColumnList,'[','source.[')+',source.[CleanCode]
				FROM 
					(SELECT DISTINCT '+@multiTMAPBKColumnList+',[CleanCode]
					 FROM 
						[stg].[Dim'+@dimTableName+'_'+@inputvar_kildeNavn+']
					) source
				LEFT JOIN
					[map].'+@dimTableName+' target
					ON ('+@multiTdimMapBK+')
				WHERE target.DW_EK_'+@dimTableName+' IS NULL
		) AS source
		ON ('+@multiTdimMapBKJoin+')
		WHEN NOT MATCHED BY TARGET THEN
			INSERT ('+@multiTMAPBKColumnList+',[CleanCode],[DW_ID_Audit_Insert],[DW_ID_Audit_Update])
			VALUES ('+REPLACE(@multiTMAPBKColumnList,'[','source.[')+',[CleanCode],@DW_ID_Audit,@DW_ID_Audit)
		;
		UPDATE STATISTICS [map].['+@dimTableName+'_'+@inputvar_kildeNavn+'];

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
		RETURN 1;

	END CATCH
'


SET @TotalPrints = (LEN(@sql) / 4000) + 1
SET @Counter=0
WHILE @Counter < @TotalPrints 
BEGIN
	PRINT(SUBSTRING(@sql,1+@Counter*4000,@Counter*4000+4000 ))
    SET @Counter = @Counter + 1
END


IF(@printonly=0) 
BEGIN 
EXEC @dbExec @SQL
END

/*Dim*/
DECLARE @UpdateInsetStatement NVARCHAR(max)

/*Tjekker om man selv har angivet historik, så antager vi at alt andet er type1. Lav selv nuværende kolonner allerede i stg hvis dette ønskes*/
IF @dimValidFromColumn IS NOT NULL
BEGIN
	IF @dimValidToColumn IS NOT NULL
	BEGIN
	/*Ny version specielt til når man beregner al historik*/
	Select @UpdateInsetStatement=
	'			/* 
				i staging tabellen er ALLE informationer om en given DW_EK.
			   Alle ikke allerede slettede rækker i dim slettemarkeres ([DW_IsDeleted] = 1)
				hvis DW_EK fra stg tabellen findes i dim  og 
				en given validfrom ikke længere eksisterer for DW_EK i dim
				(det betyder at der er ændret en ValidFrom dato i kildedata)
			*/
		

			/*Kigger efter de EKer som er i stg tabellen og deletmarkerer alle dem som ikke matcher en validFrom*/
			UPDATE [a] WITH (TABLOCK)
				SET	[a].[DW_IsDeleted] = 1
					  ,[a].[DW_IsCurrent] = 0
					  ,[a].[DW_ID_Audit_Update] = @DW_ID_Audit
			FROM [dim].['+@dimTableName+'] [a]
			INNER JOIN #stgDim'+@dimTableName+' [b]
			ON	([a].[DW_EK_'+@dimTableName+'] = [b].[DW_EK_'+@dimTableName+'])
			WHERE NOT EXISTS (
				SELECT *
				--Beregn validTo,DW_IsCUrrent,DW_IsDeleted
				FROM #stgDim'+@dimTableName+' b			  
				WHERE [a].[DW_EK_'+@dimTableName+'] = [b].[DW_EK_'+@dimTableName+']
				AND a.[DW_ValidFrom] = [b].[DW_ValidFrom]
			) 
			AND [a].[DW_IsDeleted] = 0
			/*Almindelig merge*/
			;MERGE [dim].['+@dimTableName+'] WITH (TABLOCK) AS target
				USING ( 
					SELECT [sdb].*
					FROM #stgDim'+@dimTableName+' [sdb]
				
				) AS source
			ON ([source].[DW_EK_'+@dimTableName+'] = [target].[DW_EK_'+@dimTableName+'] 
			AND [source].[DW_ValidFrom] = [target].[DW_ValidFrom])
			WHEN MATCHED AND
				(
				'+REPLACE(@dimColumnListComp, '((target',CHAR(9)+'((target')+'OR 
				([source].[DW_IsCurrent] <> [target].[DW_IsCurrent] OR ([source].[DW_IsCurrent] IS NULL AND [target].[DW_IsCurrent] IS NOT NULL) OR ([target].[DW_IsCurrent] IS NULL AND [source].[DW_IsCurrent] IS NOT NULL)) OR
				([source].[DW_ValidTo] <> [target].[DW_ValidTo] OR ([source].[DW_ValidTo] IS NULL AND [target].[DW_ValidTo] IS NOT NULL) OR ([target].[DW_ValidTo] IS NULL AND [source].[DW_ValidTo] IS NOT NULL)) OR
				([source].[DW_IsDeleted] <> [target].[DW_IsDeleted] OR [target].[DW_IsDeleted] IS NULL)
				)
			THEN UPDATE
				SET '+Replace(@dimColumnListeUpdate,',',CHAR(9)+CHAR(9)+',')+'	
					,[target].[DW_IsCurrent] = ISNULL([source].[DW_IsCurrent], [target].[DW_IsCurrent])
					,[target].[DW_ValidTo] = ISNULL([source].[DW_ValidTo],[target].[DW_ValidTo])
					,[target].[DW_ID_Audit_Update] = @DW_ID_Audit
					,[target].[DW_IsDeleted] = ISNULL([source].[DW_IsDeleted],[target].[DW_IsDeleted])

            
			WHEN NOT MATCHED BY TARGET THEN 
			INSERT (
				 [DW_EK_'+@dimTableName+']
				,[CleanCode]
				,'+Replace(@dimColumnList,',',CHAR(9)+CHAR(9)+',')+'
			
				,[DW_ValidFrom]
				,[DW_ValidTo]
				,[DW_IsCurrent]
				,[DW_ID_Audit_Insert]
				,[DW_ID_Audit_Update]
				,[DW_IsDeleted]

			)
			VALUES  (
				 [source].[DW_EK_'+@dimTableName+']
				 ,[source].[CleanCode]
				,'+Replace(REPLACE(@dimColumnList,'[','source.['),',',CHAR(9)+CHAR(9)+',')+'
			
				,[source].[DW_ValidFrom]
				,[source].[DW_ValidTo]
				,[source].[DW_IsCurrent]
				,@DW_ID_Audit
				,@DW_ID_Audit
				,[source].[DW_IsDeleted]

			)
			OUTPUT $action INTO #rowcounts;

			-- Ser en update som en ændring - rækken eksisterer allerede i dimensionen
			-- en INSERT ses som en ny række - vi kender ikke borgeren inden denne kørsel	
			SELECT	@NumberOfNewEntities =	ISNULL(SUM(CASE WHEN mergeAction = ''INSERT'' THEN 1 ELSE 0 END), 0),
						@NumberOfT1Changes  =	ISNULL(SUM(CASE WHEN mergeAction = ''UPDATE'' THEN 1 ELSE 0 END), 0)
			FROM #rowcounts  

	'
		
	END
	ELSE
	BEGIN
	Select @UpdateInsetStatement=
	'			/* 
				i staging tabellen er ALLE informationer om en given DW_EK.
			   Alle ikke allerede slettede rækker i dim slettemarkeres ([DW_IsDeleted] = 1)
				hvis DW_EK fra stg tabellen findes i dim  og 
				en given validfrom ikke længere eksisterer for DW_EK i dim
				(det betyder at der er ændret en ValidFrom dato i kildedata)
			*/
		
			IF (OBJECT_ID(''tempdb..#tempValidCurrent'')<>0)
			DROP TABLE #tempValidCurrent
			/*Beregning af ValidFrom og DW_IsCurrent samt tjek af DW_ValidFrom*/
			SELECT *
			,DW_ValidFrom=IIF('+@dimValidFromColumn+'=min('+@dimValidFromColumn+') over (PARTITION BY [DW_EK_'+@dimTableName+']),CAST(''00010101'' AS DATETEIME2(3)),'+@dimValidFromColumn+')
			,DW_ValidTo=ISNULL(LEAD('+@dimValidFromColumn+') OVER (PARTITION BY [DW_EK_'+@dimTableName+'] ORDER BY '+@dimValidFromColumn+' asc),CAST(''99990101'' AS DATETEIME2(3)))
			,DW_IsCurrent=IIF('+@dimValidFromColumn+'=max('+@dimValidFromColumn+') over (PARTITION BY [DW_EK_'+@dimTableName+']),1,0)
			,DW_IsDeleted =0
			Into #tempValidCurrent
			FROM #stgDim'+@dimTableName+'

			/*Kigger efter de EKer som er i stg tabellen og deletmarkerer alle dem som ikke matcher en validFrom*/
			UPDATE [a] WITH (TABLOCK)
				SET	[a].[DW_IsDeleted] = 1
					  ,[a].[DW_IsCurrent] = 0
					  ,[a].[DW_ID_Audit_Update] = @DW_ID_Audit
			FROM [dim].['+@dimTableName+'] [a]
			INNER JOIN #tempValidCurrent [b]
			ON	([a].[DW_EK_'+@dimTableName+'] = [b].[DW_EK_'+@dimTableName+'])
			WHERE NOT EXISTS (
				SELECT *
				--Beregn validTo,DW_IsCUrrent,DW_IsDeleted
				FROM #tempValidCurrent b			  
				WHERE [a].[DW_EK_'+@dimTableName+'] = [b].[DW_EK_'+@dimTableName+']
				AND a.[DW_ValidFrom] = [b].[DW_ValidFrom]
			) 
			AND [a].[DW_IsDeleted] = 0
			/*Almindelig merge*/
			;MERGE [dim].['+@dimTableName+'] WITH (TABLOCK) AS target
				USING ( 
					SELECT [sdb].*
					FROM #ValidCurrent [sdb]
				
				) AS source
			ON ([source].[DW_EK_'+@dimTableName+'] = [target].[DW_EK_'+@dimTableName+'] 
			AND [source].[DW_ValidFrom] = [target].[DW_ValidFrom])
			WHEN MATCHED AND
				(
				'+REPLACE(@dimColumnListComp, '((target',CHAR(9)+'((target')+'OR 
				([source].[DW_IsCurrent] <> [target].[DW_IsCurrent] OR ([source].[DW_IsCurrent] IS NULL AND [target].[DW_IsCurrent] IS NOT NULL) OR ([target].[DW_IsCurrent] IS NULL AND [source].[DW_IsCurrent] IS NOT NULL)) OR
				([source].[DW_ValidTo] <> [target].[DW_ValidTo] OR ([source].[DW_ValidTo] IS NULL AND [target].[DW_ValidTo] IS NOT NULL) OR ([target].[DW_ValidTo] IS NULL AND [source].[DW_ValidTo] IS NOT NULL)) OR
				([source].[DW_IsDeleted] <> [target].[DW_IsDeleted] OR [target].[DW_IsDeleted] IS NULL)
				)
			THEN UPDATE
				SET '+Replace(@dimColumnListeUpdate,',',CHAR(9)+CHAR(9)+',')+'	
					,[target].[DW_IsCurrent] = ISNULL([source].[DW_IsCurrent], [target].[DW_IsCurrent])
					,[target].[DW_ValidTo] = ISNULL([source].[DW_ValidTo],[target].[DW_ValidTo])
					,[target].[DW_ID_Audit_Update] = @DW_ID_Audit
					,[target].[DW_IsDeleted] = ISNULL([source].[DW_IsDeleted],[target].[DW_IsDeleted])

            
			WHEN NOT MATCHED BY TARGET THEN 
			INSERT (
				 [DW_EK_'+@dimTableName+']
				,[CleanCode]
				,'+Replace(@dimColumnList,',',CHAR(9)+CHAR(9)+',')+'
			
				,[DW_ValidFrom]
				,[DW_ValidTo]
				,[DW_IsCurrent]
				,[DW_ID_Audit_Insert]
				,[DW_ID_Audit_Update]
				,[DW_IsDeleted]

			)
			VALUES  (
				 [source].[DW_EK_'+@dimTableName+']
				 ,[source].[CleanCode]
				,'+Replace(REPLACE(@dimColumnList,'[','source.['),',',CHAR(9)+CHAR(9)+',')+'
			
				,[source].[DW_ValidFrom]
				,[source].[DW_ValidTo]
				,[source].[DW_IsCurrent]
				,@DW_ID_Audit
				,@DW_ID_Audit
				,[source].[DW_IsDeleted]

			)
			OUTPUT $action INTO #rowcounts;

			-- Ser en update som en ændring - rækken eksisterer allerede i dimensionen
			-- en INSERT ses som en ny række - vi kender ikke borgeren inden denne kørsel	
			SELECT	@NumberOfNewEntities =	ISNULL(SUM(CASE WHEN mergeAction = ''INSERT'' THEN 1 ELSE 0 END), 0),
						@NumberOfT1Changes  =	ISNULL(SUM(CASE WHEN mergeAction = ''UPDATE'' THEN 1 ELSE 0 END), 0)
			FROM #rowcounts  

	'
	END
END
ELSE
/*Hvis man ikke har angiver en valifrom så start SP dim.UpdateXXX her*/
(Select @UpdateInsetStatement='
		/*Type 1 ændringer*/
		DECLARE @nu DATETIME2(2) =cast(SYSDATETIME() As DATETIME2(2));

		UPDATE	target WITH (TABLOCK)
		SET		[DW_ID_Audit_Update] = @DW_ID_Audit
				,'+Replace(@dimColumnListeUpdateT1,',',CHAR(9)+CHAR(9)+CHAR(9)+',')+'
		  FROM 
				dim.'+@dimTableName+' as target 
		  INNER JOIN 
				(SELECT 
				*
				,ROW_NUMBER() OVER (PARTITION BY DW_EK_'+@dimTableName+' ORDER BY [CleanCode]) AS RN 
				 FROM #stgDim'+@dimTableName+'
				) AS source
				ON (target.DW_EK_'+@dimTableName+'=source.DW_EK_'+@dimTableName+' AND source.RN=1)
		  WHERE (
				'+REPLACE(@dimColumnListCompT1, '((target',CHAR(9)+CHAR(9)+'((target')+'
			  )
			  SET @NumberOfT1Changes=@@ROWCOUNT
	
	' --Tager højde for at der ingen type2 kolonner er
	+IIF(@dimColumnListCompT2 IS NULL,'',
	'
	/*type 2 ændringer*/

		INSERT INTO dim.'+@dimTableName+' WITH (TABLOCK) (
			 [DW_EK_'+@dimTableName+']
			,[CleanCode]
			,'+Replace(@dimColumnList,',',CHAR(9)+CHAR(9)+',')+'
			,[DW_ValidFrom]
			,[DW_IsCurrent]
			,[DW_ID_Audit_Insert]
			,[DW_ID_Audit_Update]

			   )
		SELECT   m_out.[DW_EK_'+@dimTableName+']
				,m_out.[CleanCode]
				,'+REPLACE(REPLACE(@dimColumnList,',',CHAR(9)+CHAR(9)+CHAR(9)+','),'[','m_out.[')+'
				,@nu
				,1
				,@DW_ID_Audit
				,@DW_ID_Audit 

		FROM
		(
			MERGE dim.'+@dimTableName+' WITH (TABLOCK) AS target 
			USING #stgDim'+@dimTableName+' as source
			ON (target.DW_EK_'+@dimTableName+' = source.DW_EK_'+@dimTableName+' AND target.DW_IsCurrent = 1)
			WHEN MATCHED
				AND (
					'+@dimColumnListCompT2+'
					)
				THEN
				UPDATE
					SET DW_IsCurrent = 0,
						DW_ValidTo = @nu,
						DW_ID_Audit_Update = @DW_ID_Audit
				OUTPUT $action Action_out 
						,source.[DW_EK_'+@dimTableName+']
						,source.[CleanCode]
						,'+REPLACE(REPLACE(@dimColumnList,',',CHAR(9)+CHAR(9)+CHAR(9)+CHAR(9)+CHAR(9)+','),'[','source.[')+'
		  ) m_out
		  WHERE m_out.Action_out = ''UPDATE'' 
		  
		  SET @NumberOfT2Changes=@@ROWCOUNT

	')+'
			/*Nye entiteter*/
		INSERT INTO [dim].['+@dimTableName+'] WITH (TABLOCK)
		(
			 [DW_EK_'+@dimTableName+']
			 ,[CleanCode]
			,'+REPLACE(@dimColumnList,',',CHAR(9)+CHAR(9)+',')+'
			,[DW_ID_Audit_Insert]
			,[DW_ID_Audit_Update]
		)
		SELECT 
				 source.[DW_EK_'+@dimTableName+']
				 ,source.[CleanCode]
				,'+REPLACE(REPLACE(@dimColumnListCoalesce,',',CHAR(9)+CHAR(9)+CHAR(9)+','),'COALESCE(','COALESCE(source.')+'
				,@DW_ID_Audit
				,@DW_ID_Audit
			   FROM #stgDim'+@dimTableName+' source
				LEFT JOIN
				dim.'+@dimTableName+' target
				ON (source.DW_EK_'+@dimTableName+' = target.DW_EK_'+@dimTableName+')
				WHERE target.DW_EK_'+@dimTableName+' IS NULL

		SET @numberOfNewEntities=@@ROWCOUNT;
');
/*Indæstter afterburn logik her. Det er nødvendigt ved type2 eller type6 Hist*/
DECLARE @AfterBurn NVARCHAR(max)
set @dimColumnListNuv= isnull(@dimColumnListNuv,'')
/*tjekker for type6 hist. Ved typ6 hist så består afterburn af update af nuværende kolonner, samt update af ChangedColumns*/
IF @dimColumnListNuv <>''
(
	Select @AfterBurn=
N'
	Update target  
	SET ChangedColumns= IIF( source.[DW_SK_'+@dimTableName+'] IS NULL, ''NoChange'',
								'+
								@dimColumnListChgRT2
								+'
							)
	'+@dimColumnListNuv+'
	FROM dim.'+@dimTableName+' AS target
	LEFT JOIN dim.'+@dimTableName+' AS Source
	ON target.[DW_EK_'+@dimTableName+']	=Source.[DW_EK_'+@dimTableName+'] AND target.DW_ValidFrom=Source.DW_ValidTo and source.[DW_IsDeleted]=0
	LEFT JOIN dim.'+@dimTableName+' AS nuv
	ON nuv.[DW_EK_'+@dimTableName+']=target.[DW_EK_'+@dimTableName+'] AND nuv.[DW_IsCurrent]=1
	--Kiger på EKere der allerede er blevet ramt i denne kørsel
	WHERE target.DW_ID_Audit_Update=@DW_ID_Audit
	'
	 

);
/*Hvis der ikke er HIST type6 og der er HIST type 2 så opdateres changeColumns*/
ELSE
(
Select @AfterBurn=N'
	Update target  
	SET ChangedColumns= IIF( source.[DW_SK_'+@dimTableName+'] IS NULL, ''NoChange'',
								'+
								@dimColumnListChgRT2
								+'
							)
	FROM dim.'+@dimTableName+' AS target
	LEFT JOIN dim.'+@dimTableName+' AS Source
	ON target.[DW_EK_'+@dimTableName+']	=Source.[DW_EK_'+@dimTableName+'] AND target.DW_ValidFrom=Source.DW_ValidTo and source.[DW_IsDeleted]=0
	--Kiger på EKere der allerede er blevet ramt i denne kørsel
	WHERE target.DW_ID_Audit_Update=@DW_ID_Audit
	'

);
--Afterburn er null hvis der ikke er type 2 kolonner
SET @AfterBurn=ISNULL(@AfterBurn,'')

/*Logik tag*/
SET @def=''
/*Default logik*/
SET @logik=
N'IF (OBJECT_ID(''tempdb..#stgDim'+@dimTableName+''')<>0)
		DROP TABLE #stgDim'+@dimTableName+'

	SELECT Distinct 
	target.DW_EK_'+@dimTableName+'
	,source.[CleanCode]
	,'+REPLACE(REPLACE(@dimColumnList,'[','source.['),',',CHAR(9)+',')+
	'	INTO #stgDim'+@dimTableName+'
	FROM stg.[Dim'+@dimTableName+'_'+@inputvar_kildeNavn+'] as source
	JOIN map.['+@dimTableName+'_'+@inputvar_kildeNavn+'] as target
	ON ('+@multiTdimMapBKJoin+')'
	/*Kigger efter logik tag i starten af sp dim.UpdateXXX*/
IF(OBJECT_ID(N''+@inputvar_DSOdatabase+'.dim.Update'+@dimTableName) IS NOT NULL)
	BEGIN
		
		Set @SQL = N'SELECT @def= OBJECT_DEFINITION( OBJECT_ID(N'''+@inputvar_DSOdatabase+'.dim.Update'+@dimTableName+'''))'
		exec @dbExec @SQL,N'@def nvarchar(max) out', @def out
	
		SELECT @logik=SUBSTRING(@def
			,LEN('/*LOGIK START*/')+CHARINDEX('/*LOGIK START*/',@def)
			,-(1+LEN('/*LOGIK SLUT*/'))+CHARINDEX('/*LOGIK SLUT*/',@def)-CHARINDEX('/*LOGIK START*/',@def)
			)
	END 



/*Kigger efter Afterburn logik tags i sp dim.UpdateXXX*/
declare @logikAfterBurn nvarchar(max)
SET @def=''
SET @logikAfterBurn=N''
IF(OBJECT_ID(N''+@inputvar_DSOdatabase+'.dim.Update'+@dimTableName) IS NOT NULL)
	BEGIN
				
		Set @SQL = N'SELECT @def= OBJECT_DEFINITION( OBJECT_ID(N'''+@inputvar_DSOdatabase+'.dim.Update'+@dimTableName+'''))'
		exec @dbExec @SQL,N'@def nvarchar(max) out', @def out

		SELECT @logikAfterBurn=SUBSTRING(@def
			,LEN('/*LOGIKAFTERBURN START*/')+CHARINDEX('/*LOGIKAFTERBURN START*/',@def)
			,-(1+LEN('/*LOGIKAFTERBURN SLUT*/'))+CHARINDEX('/*LOGIKAFTERBURN SLUT*/',@def)-CHARINDEX('/*LOGIKAFTERBURN START*/',@def)
			)
	END 

/*samler sp dim.UpdateXXX sammen*/
SET @sql =
''+IIF(OBJECT_ID (''+@inputvar_DSOdatabase+'.dim.Update'+@dimTableName) IS NULL,'CREATE','ALTER') +' PROCEDURE [dim].[Update'+@dimTableName+']
	@DW_ID_Audit [INT],
	@DW_ID_Audit_EndValue_Last INT = 0,
	@NumberOfNewEntities [BIGINT] = 0 OUTPUT,
	@NumberOfT1Changes [BIGINT] = 0 OUTPUT,
	@NumberOfT2Changes [BIGINT] = 0 OUTPUT
WITH EXECUTE AS CALLER
AS
SET NOCOUNT ON;
BEGIN TRY

	DECLARE @TranCounter INT = @@TRANCOUNT, @SavePoint NVARCHAR(32) = CAST(@@PROCID AS NVARCHAR(20)) + N''_'' + CAST(@@NESTLEVEL AS NVARCHAR(2));


	CREATE TABLE #rowcounts (mergeAction nvarchar(10));

		SET @NumberOfT1Changes = 0
		SET @NumberOfT2Changes = 0
		SET @NumberOfNewEntities = 0
		SET @DW_ID_Audit = ISNULL(@DW_ID_Audit, 1)
		SET @DW_ID_Audit_EndValue_Last = ISNULL(@DW_ID_Audit_EndValue_Last, 1)

/*LOGIK START*/'
+@logik+
'/*LOGIK SLUT*/
	

	IF (OBJECT_ID(''tempdb..#temptable'')<>0)
		DROP TABLE #temptable

   CREATE TABLE #temptable (
	[DW_SK_'+@dimTableName+'] INT NOT NULL,
	[DW_EK_'+@dimTableName+'] INT NOT NULL,
	[CleanCode] nvarchar(200) NOT NULL,
	'+REPLACE(@dimNuvColumnDTypeList,';',',')+',
	[DW_ID_Audit_Insert] INT NOT NULL,
	[DW_ID_Audit_Update] INT NOT NULL
	) 

	INSERT	INTO #temptable
	VALUES	(
	-1
	,-1
	,''Ukendt''
	,'+Replace(@dimUkendtListe,',',CHAR(9)+',')+
	+CHAR(9)+',@DW_ID_Audit
	,@DW_ID_Audit) 

	IF @TranCounter > 0
		SAVE TRANSACTION @SavePoint
	ELSE
		BEGIN TRANSACTION

	/* 
		Start på opdatering af -1,-2 nøgler 
		Opdatering af -1-2 nøgler nu en del af den alm. dimensions opdatering, herved kan vi påsætte en valid auditnøgle, som ikke driller ift. bred tabeller og delta loads
		OBS! -1,-2 Rækkerne må kun opdateres hvis der er ændringer til dem, ellers får vi problemer med audit nøglerne og brede tabeller med delta load igen!!! 
	*/	
	SET IDENTITY_INSERT [dim].['+@dimTableName+'] ON;

	MERGE	[dim].['+@dimTableName+'] WITH (TABLOCK) target 
	USING [#temptable] source
		ON source.[DW_SK_'+@dimTableName+'] = target.[DW_SK_'+@dimTableName+']
	WHEN MATCHED AND 
	(([source].[DW_EK_'+@dimTableName+'] <> [target].[DW_EK_'+@dimTableName+'] OR ([source].[DW_EK_'+@dimTableName+'] IS NULL AND [target].[DW_EK_'+@dimTableName+'] IS NOT NULL) OR ([target].[DW_EK_'+@dimTableName+'] IS NULL AND [source].[DW_EK_'+@dimTableName+'] IS NOT NULL)) OR 
	'+REPLACE(@dimColumnListComp, '((target',CHAR(9)+'((target')+
	')

	THEN UPDATE SET 
		[DW_EK_'+@dimTableName+'] = [source].[DW_EK_'+@dimTableName+']
		,[CleanCode]=[source].[CleanCode]
		,'+Replace(@dimColumnListeUpdate,',',CHAR(9)+CHAR(9)+',')+'
	WHEN NOT MATCHED BY TARGET THEN 
	INSERT
	(	 [DW_SK_'+@dimTableName+']
		,[DW_EK_'+@dimTableName+']
		,[CleanCode]
		,'+REPLACE(@dimColumnList,',',CHAR(9)+CHAR(9)+',')+'
		,[DW_ID_Audit_Insert]
		,[DW_ID_Audit_Update]
	)
	VALUES
	(	 [source].[DW_SK_'+@dimTableName+']
		,[source].[DW_EK_'+@dimTableName+']
		,[source].[CleanCode]
		,'+Replace(Replace(@dimColumnList,'[','[source].['),',',CHAR(9)+CHAR(9)+',')+'
		,@DW_ID_Audit
		,@DW_ID_Audit
	);		

	SET IDENTITY_INSERT [dim].['+@dimTableName+'] OFF;
	/* Slut på opdatering af -1,-2 nøgler */	
	/*Skifter afhængig af type2*/
	'+@UpdateInsetStatement+'

	'+@AfterBurn+'
	/*Special logik til AfterBurn*/
	
	/*LOGIKAFTERBURN START*/'
	+@logikAfterBurn+
	'/*LOGIKAFTERBURN SLUT*/


	UPDATE STATISTICS dim.'+@dimTableName+';   

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
	RETURN 1;
END CATCH
'

SET @TotalPrints = (LEN(@sql) / 4000) + 1
SET @Counter=0
WHILE @Counter < @TotalPrints 
BEGIN
	PRINT(SUBSTRING(@sql,1+@Counter*4000,@Counter*4000+4000 ))
    SET @Counter = @Counter + 1
END

IF(@printonly=0) 
BEGIN 
EXEC @dbExec @SQL
END
select ('Husk at rette map view og dim.updateXXX vi multiple map-tabeller til samme dimension ')




/*
Need to have
	-Validering
	-Mangler det noget standardkode?
	-Index optimering på staging tables

Nice to have
	
	-Understøtte sharedDimensions
		--map view lav logik tag
		--dim.update lav logik tag for og efter merge/update/Insert
	-Dim		
		--Dpa views til alle fcts og dims der laver isnull på alle kolonner
			--Ændring af isnull på kolonner (ikke dw_sk_) (Version til Eget miljø)

	-Beautify
	-dokumenter
	-opskrift
	-Kun opdatering af tabeller (Mest til eksisternde kode)
*/

END