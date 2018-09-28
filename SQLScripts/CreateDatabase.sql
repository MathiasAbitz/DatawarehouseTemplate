USE [DWFramework]
GO
CREATE PROCEDURE [dbo].[CreateDatabase]
@DatabaseNavn nvarchar(100) -- Navnet på den database der skal oprettes
AS

Begin



Declare @InitialDataFileSizeMB INT = 100 -- Initiel Størrelse i MB(1000=1GB) for Data Filen i Standard Data Gruppen 
Declare @DataFileGrowthMB INT = 100 -- Størrelse i MB (1000=1GB) som Data Filen skal vokse med når de løber fuld 
Declare @InitialLogFileSizeMB INT = 100 -- Initiel Størrelse i MB(1000=1GB) af Log Filen (default dobbelt af en data fil initielle størrelse)
Declare @LogFileGrowthMB INT = 100 -- Størrelse i MB(1000=1GB) som log filen vokser med når den løber fuld (default dobbelt af data fil growth)
Declare @DefaultData nvarchar(512)
Declare @DefaultData1 nvarchar(512)
Declare @DefaultData2 nvarchar(512)
Declare @DefaultData3 nvarchar(512)
Declare @DefaultData4 nvarchar(512)
Declare @DefaultLog nvarchar(512)
Declare @MasterData nvarchar(512)
Declare @MasterLog nvarchar(512)
Declare @Sql nvarchar(max) 

SET @DatabaseNavn = LTRIM(RTRIM(@DatabaseNavn))


/*

( NAME = N'DSO_MachineLearning_CDSS', FILENAME = N'H:\DEVSQL\DATA\DSO_MachineLearning_CDSS.mdf' , SIZE = 8192KB , FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'DSO_MachineLearning_CDSS_log', FILENAME = N'G:\DEVSQL\LOG\DSO_MachineLearning_CDSS_log.ldf' , SIZE = 37888KB , FILEGROWTH = 65536KB )

*/

-- Hvis databasen allerede findes, stopper vi
IF  EXISTS (SELECT name FROM sys.databases WHERE name = @DatabaseNavn)
	begin
		Print ('Databasen [' + @DatabaseNavn + '] findes allerede')
		return
	end

-- Her findes default installationsfolderne for datafiler og logfiler
exec master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultData', @DefaultData OUTPUT
exec master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultLog', @DefaultLog OUTPUT
exec master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer\Parameters', N'SqlArg0', @MasterData OUTPUT

select @MasterData=substring(@MasterData, 3, 255)
select @MasterData=substring(@MasterData, 1, len(@MasterData) - charindex('\', reverse(@MasterData)))
exec master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer\Parameters', N'SqlArg2', @MasterLog output
select @MasterLog=substring(@MasterLog, 3, 255)
select @MasterLog=substring(@MasterLog, 1, len(@MasterLog) - charindex('\', reverse(@MasterLog)))

set @DefaultData = isnull(@DefaultData, @MasterData) 
Set @DefaultLog = isnull(@DefaultLog, @MasterLog) 
Set @DefaultData1 =  @DefaultData

Set @Sql = N'
CREATE DATABASE [' + @DatabaseNavn + ']
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N''PRIMARY'', FILENAME = N''' + @DefaultData + '\' + @DatabaseNavn + '_PRIMARY.mdf'' , SIZE = 50MB , MAXSIZE = 500MB, FILEGROWTH = 50MB ), 
 FILEGROUP [Standard]  DEFAULT 
( NAME = N''Standard_01'', FILENAME = N''' + @DefaultData1 + '\' + @DatabaseNavn + '_Standard_01.ndf'' , SIZE = ' + CAST(@InitialDataFileSizeMB*1024 as NVARCHAR) + 'KB , MAXSIZE = UNLIMITED, FILEGROWTH = ' + CAST(@DataFileGrowthMB*1024 as NVARCHAR) + 'KB ), 
 FILEGROUP [Staging]   
( NAME = N''Staging_01'', FILENAME = N''' + @DefaultData1 + '\' + @DatabaseNavn + '_Staging_01.ndf'' , SIZE =  100MB , MAXSIZE = UNLIMITED, FILEGROWTH = 100MB )
 LOG ON 
( NAME = N''Log'', FILENAME = N'''+@DefaultLog+'\' + @DatabaseNavn + '_Log.ldf'' , SIZE = ' + CAST(@InitialLogFileSizeMB*1024 as NVARCHAR) + 'KB , MAXSIZE = 2048GB , FILEGROWTH = ' + CAST(@LogFileGrowthMB*1024 as NVARCHAR) + 'KB )
 COLLATE Danish_Norwegian_CI_AS

IF (1 = FULLTEXTSERVICEPROPERTY(''IsFullTextInstalled''))
begin
EXEC [' + @DatabaseNavn + '].[dbo].[sp_fulltext_database] @action = ''enable''
end ' + CHAR(13)

SET @Sql += 
N'ALTER DATABASE [' + @DatabaseNavn + '] SET ANSI_NULL_DEFAULT ON 
ALTER DATABASE [' + @DatabaseNavn + '] SET ANSI_NULLS ON 
ALTER DATABASE [' + @DatabaseNavn + '] SET ANSI_PADDING ON 
ALTER DATABASE [' + @DatabaseNavn + '] SET ANSI_WARNINGS ON 
ALTER DATABASE [' + @DatabaseNavn + '] SET ARITHABORT ON 
ALTER DATABASE [' + @DatabaseNavn + '] SET AUTO_CLOSE OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET AUTO_CREATE_STATISTICS ON 
ALTER DATABASE [' + @DatabaseNavn + '] SET AUTO_SHRINK OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET AUTO_UPDATE_STATISTICS ON 
ALTER DATABASE [' + @DatabaseNavn + '] SET CURSOR_CLOSE_ON_COMMIT OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET CURSOR_DEFAULT  LOCAL 
ALTER DATABASE [' + @DatabaseNavn + '] SET CONCAT_NULL_YIELDS_NULL ON 
ALTER DATABASE [' + @DatabaseNavn + '] SET NUMERIC_ROUNDABORT OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET QUOTED_IDENTIFIER ON 
ALTER DATABASE [' + @DatabaseNavn + '] SET RECURSIVE_TRIGGERS OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET DISABLE_BROKER 
ALTER DATABASE [' + @DatabaseNavn + '] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET DATE_CORRELATION_OPTIMIZATION OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET TRUSTWORTHY OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET ALLOW_SNAPSHOT_ISOLATION OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET PARAMETERIZATION SIMPLE 
ALTER DATABASE [' + @DatabaseNavn + '] SET READ_COMMITTED_SNAPSHOT OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET HONOR_BROKER_PRIORITY OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET RECOVERY SIMPLE 
ALTER DATABASE [' + @DatabaseNavn + '] SET MULTI_USER 
ALTER DATABASE [' + @DatabaseNavn + '] SET PAGE_VERIFY CHECKSUM  
ALTER DATABASE [' + @DatabaseNavn + '] SET DB_CHAINING OFF 
ALTER DATABASE [' + @DatabaseNavn + '] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
ALTER DATABASE [' + @DatabaseNavn + '] SET TARGET_RECOVERY_TIME = 0 SECONDS 
ALTER DATABASE [' + @DatabaseNavn + '] SET READ_WRITE
ALTER DATABASE [' + @DatabaseNavn + '] MODIFY FILEGROUP [PRIMARY] AUTOGROW_ALL_FILES
ALTER DATABASE [' + @DatabaseNavn + '] MODIFY FILEGROUP [Standard] AUTOGROW_ALL_FILES
ALTER DATABASE [' + @DatabaseNavn + '] MODIFY FILEGROUP [Staging] AUTOGROW_ALL_FILES
'
Print(@sql)
EXEC(@Sql)
END