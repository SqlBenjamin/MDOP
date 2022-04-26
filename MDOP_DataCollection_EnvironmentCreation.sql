USE [master];
GO
SET NOCOUNT ON;
GO

/***************************************************************************************************************
Purpose: This creates the MDOP_Test database and the job that collects data for testing MDOP values.

History:
Date          Version    Author                   Notes:
04/25/2022    0.0        Benjamin Reynolds        Uploading to GitHub.
*****************************************************************************************************************/

-- Declare Working Variables:
DECLARE  @DefLog        nvarchar(512)
        ,@DefMdf        nvarchar(512)
        ,@Mdfi          tinyint
        ,@Ldfi          tinyint
        ,@Arg           nvarchar(10)
        ,@CreateDB      nvarchar(max)
        ,@ErrorMessage  nvarchar(4000)
        ,@ErrorNumber   int
        ,@ErrorSeverity int
        ,@ErrorState    int
        ,@ReturnCode    int
        ,@JobId         binary(16)
        ,@CMDB          sysname;

SET @CreateDB = N'CREATE DATABASE [MDOP_Test]
    ON PRIMARY ( NAME = MDOP_Test
                ,FILENAME = N''@DefMdfMDOP_Test.mdf''
                ,SIZE = 1GB
                ,FILEGROWTH = 512MB
                )
              ,( NAME = MDOP_Test_1
                ,FILENAME = N''@DefMdfMDOP_Test_1.ndf''
                ,SIZE = 1GB
                ,FILEGROWTH = 512MB
                )
        LOG ON ( NAME = MDOP_Test_log
                ,FILENAME = N''@DefLogMDOP_Test_log.ldf''
                ,SIZE = 512MB
                ,FILEGROWTH = 512MB
                )
COLLATE SQL_Latin1_General_CP1_CI_AS;';

SELECT TOP 1 @CMDB = name
  FROM sys.databases
 WHERE name LIKE N'CM[_]___'
   AND state = 0;

-- Create the MDOP_Test database:
IF DB_ID(N'MDOP_Test') IS NOT NULL
BEGIN
    PRINT N'MDOP_Test Already Exists; not running this script.';
    PRINT N'Drop the database before running this again if that is the intent.';
    GOTO EndScript;
END;

IF @CMDB IS NULL
BEGIN
    PRINT N'There is no CM database in the ONLINE state on this instance!';
    PRINT N'Run this only on SQL Servers running CM.';
    GOTO EndScript;
END;
ELSE
BEGIN
    -- Get the Default MDF location (from the registry):
    EXECUTE master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultData', @DefMdf OUTPUT, 'no_output';
    IF @DefMdf IS NULL -- if we couldn't get the key from this location for some reason then look at the startup parameters:
    BEGIN
        SET @Mdfi = 0;
        WHILE @Mdfi < 100
        BEGIN
            SELECT @Arg = N'SQLArg' + CAST(@Mdfi AS nvarchar(4));
            EXECUTE master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer\Parameters', @Arg, @DefMdf OUTPUT, 'no_output';
            IF LOWER(LEFT(REVERSE(@DefMdf),10)) = N'fdm.retsam'
            BEGIN
                -- If we found the parameter for the master data file then set the variable and stop processing this loop:
                SELECT @DefMdf = SUBSTRING(@DefMdf,3,CHARINDEX(N'\master.mdf',@DefMdf)-3);
                BREAK;
            END;
            ELSE
            SET @DefMdf = NULL;

            SELECT @Mdfi += 1;
        END;
    END;
    IF @DefMdf IS NOT NULL AND LEFT(REVERSE(@DefMdf),1) != N'\'
    SET @DefMdf += N'\';

    -- Get the Default LDF location (from the registry):
    EXECUTE master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer', N'DefaultLog', @DefLog OUTPUT, 'no_output';
    IF @DefLog IS NULL -- if we couldn't get the key from this location for some reason then look at the startup parameters:
    BEGIN
        SET @Ldfi = 0;
        WHILE @Ldfi < 100
        BEGIN
            SELECT @Arg = N'SQLArg' + CAST(@Ldfi AS nvarchar(4));
            EXECUTE master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\Microsoft\MSSQLServer\MSSQLServer\Parameters', @Arg, @DefLog OUTPUT, 'no_output';
            IF LOWER(LEFT(REVERSE(@DefLog),11)) = N'fdl.goltsam'
            BEGIN
                -- If we found the parameter for the master log file then set the variable and stop processing this loop:
                SELECT @DefLog = SUBSTRING(@DefLog,3,CHARINDEX(N'\mastlog.ldf',@DefLog)-3);
                BREAK;
            END;
            ELSE
            SET @DefLog = NULL;

            SELECT @Ldfi += 1;
        END;
    END;
    IF @DefLog IS NOT NULL AND LEFT(REVERSE(@DefLog),1) != N'\'
    SET @DefLog += N'\';

    IF @DefMdf IS NOT NULL AND @DefLog IS NOT NULL
    BEGIN
        BEGIN TRY
            SELECT @CreateDB = REPLACE(REPLACE(@CreateDB,N'@DefMdf',@DefMdf),N'@DefLog',@DefLog);
            EXECUTE (@CreateDB);
            PRINT N'Database Created';
        END TRY
	    BEGIN CATCH
		    SELECT  @ErrorMessage  = ERROR_MESSAGE()
                   ,@ErrorSeverity = ERROR_SEVERITY()
                   ,@ErrorState    = ERROR_STATE()
                   ,@ErrorNumber   = ERROR_NUMBER();

            PRINT N'Database Creation Error Occurred!';
            PRINT N' *** Error Number: '+CONVERT(nvarchar(20),@ErrorNumber);
            PRINT N' *** Error Message: '+@ErrorMessage;

            RAISERROR ( @ErrorMessage
                       ,@ErrorSeverity
                       ,@ErrorState
                       ) WITH NOWAIT;
            
            GOTO DBNotCreated;
	    END CATCH;
    END;
    ELSE
    BEGIN
        PRINT N'';
        PRINT N'***********************************************************************';
        PRINT N'Database CANNOT be created!';
        PRINT N' *** The default data file or log file location was not found!';
        PRINT N' **** Default Data File location found: '+ISNULL(@DefMdf,N'NULL');
        PRINT N' **** Default Log File location found: '+ISNULL(@DefLog,N'NULL');
        PRINT N'***********************************************************************';
        PRINT N'';
        GOTO DBNotCreated;
    END;

    -- Change the owner of the DB now that it has been created:
    ALTER AUTHORIZATION ON DATABASE::MDOP_Test TO sa;
    PRINT N'Owner/Authorization updated to "sa".';
END;

-- Now that the database is created we need to create the objects:
 -- Only create DRS tables if change tracking is enabled for the CM database:
IF EXISTS (
SELECT *
  FROM sys.change_tracking_databases ctd
       INNER JOIN sys.databases dbs
          ON ctd.database_id = dbs.database_id
         AND dbs.name = @CMDB
)
EXECUTE (N'USE [MDOP_Test];
CREATE TABLE dbo.DRSCountInfo ( RecID int IDENTITY(1,1) PRIMARY KEY NOT NULL
                               ,BatchId int NOT NULL
                               ,GlobalQueueCount int NOT NULL
                               ,SiteQueueCount int NOT NULL
                               ,OutgoingQueueCount int NOT NULL
                               ,OutdatedDrsCount int NOT NULL
                               ,MDOPSetting tinyint NOT NULL
                               ,CaptureDateTimeUTC datetime NOT NULL DEFAULT GETUTCDATE()
                               );
CREATE TABLE dbo.SysCommitInfo ( RecID int IDENTITY(1,1) PRIMARY KEY NOT NULL
                                ,BatchId int NOT NULL
                                ,rows char(20) NOT NULL
                                ,reserved varchar(18) NOT NULL
                                ,data varchar(18) NOT NULL
                                ,index_size varchar(18) NOT NULL
                                ,unused varchar(18) NOT NULL
                                ,MDOPSetting tinyint NOT NULL
                                ,CaptureDateTimeUTC datetime NOT NULL DEFAULT GETUTCDATE()
                                );
CREATE TABLE dbo.DrsReplGroupDelay ( RecID int IDENTITY(1,1) PRIMARY KEY NOT NULL
                                    ,BatchId int NOT NULL
                                    ,CurrentSite nvarchar(3) NOT NULL
                                    ,TargetSite nvarchar(3) NOT NULL
                                    ,ReplicationGroup nvarchar(255) NOT NULL
                                    ,SecondsOld int NOT NULL
                                    ,SumChangeCount int NOT NULL
                                    ,SumMessageCount int NOT NULL
                                    ,MDOPSetting tinyint NOT NULL
                                    ,CaptureDateTimeUTC datetime NOT NULL DEFAULT GETUTCDATE()                                    
                                    );');

-- Create these tables for all cases:
EXECUTE (N'USE [MDOP_Test];
CREATE TABLE dbo.OsSpinlockStats ( RecID int IDENTITY(1,1) PRIMARY KEY NOT NULL
                                  ,BatchId int NOT NULL
                                  ,name sysname NOT NULL
                                  ,collisions bigint NOT NULL
                                  ,spins bigint NOT NULL
                                  ,spins_per_collision real NOT NULL
                                  ,sleep_time bigint NOT NULL
                                  ,backoffs int NOT NULL
                                  ,MDOPSetting tinyint NOT NULL
                                  ,CaptureDateTimeUTC datetime NOT NULL DEFAULT GETUTCDATE()
                                  );
CREATE TABLE dbo.OsWaitStats ( RecID int IDENTITY(1,1) PRIMARY KEY NOT NULL
                              ,BatchId int NOT NULL
                              ,wait_type nvarchar(60) NOT NULL
                              ,waiting_tasks_count bigint NOT NULL
                              ,wait_time_ms bigint NOT NULL
                              ,max_wait_time_ms bigint NOT NULL
                              ,signal_wait_time_ms bigint NOT NULL
                              ,MDOPSetting tinyint NOT NULL
                              ,CaptureDateTimeUTC datetime NOT NULL DEFAULT GETUTCDATE()
                              );
CREATE TABLE dbo.OsWaitingTasks ( RecID int IDENTITY(1,1) PRIMARY KEY NOT NULL
                                 ,BatchId int NOT NULL
                                 ,waiting_task_address varbinary(8) NOT NULL
                                 ,session_id smallint NULL
                                 ,exec_context_id int NULL
                                 ,wait_duration_ms bigint NOT NULL
                                 ,wait_type nvarchar(60) NOT NULL
                                 ,resource_address varbinary(8) NULL
                                 ,blocking_task_address varbinary(8) NULL
                                 ,blocking_session_id smallint NULL
                                 ,blocking_exec_context_id int NULL
                                 ,resource_description nvarchar(3072) NULL
                                 ,MDOPSetting tinyint NOT NULL
                                 ,CaptureDateTimeUTC datetime NOT NULL DEFAULT GETUTCDATE()
                                 );
CREATE TABLE dbo.OsSchedulers ( RecID int IDENTITY(1,1) PRIMARY KEY NOT NULL
                               ,BatchId int NOT NULL
                               ,scheduler_id int NOT NULL
                               ,cpu_id smallint NOT NULL
                               ,status nvarchar(60) NOT NULL
                               ,is_online bit NOT NULL
                               ,is_idle bit NOT NULL
                               ,parent_node_id int NOT NULL
                               ,current_tasks_count int NOT NULL
                               ,runnable_tasks_count int NOT NULL
                               ,current_workers_count int NOT NULL
                               ,active_workers_count int NOT NULL
                               ,work_queue_count bigint NOT NULL
                               ,pending_disk_io_count int NOT NULL
                               ,preemptive_switches_count int NOT NULL
                               ,context_switches_count int NOT NULL
                               ,yield_count int NOT NULL
                               ,load_factor int NOT NULL
                               ,total_scheduler_delay_ms bigint NOT NULL
                               ,MDOPSetting tinyint NOT NULL
                               ,CaptureDateTimeUTC datetime NOT NULL DEFAULT GETUTCDATE()
                               );');

-- Now let's create the jobs to start collecting the data:
USE [msdb];
-- Cleanup any old job(s) first:
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs_view WHERE name = N'MDOP_DataCollector')
EXECUTE msdb.dbo.sp_delete_job @job_name = N'MDOP_DataCollector', @delete_unused_schedule = 1;

-- Now Let's create a new job category if it doesn't exist:
IF NOT EXISTS (SELECT * FROM msdb.dbo.syscategories WHERE name = N'Data Collector' AND category_class = 1)
BEGIN
    EXECUTE msdb.dbo.sp_add_category @class = N'JOB', @type = N'LOCAL', @name = N'Data Collector';
END;

-- Create the Job:
BEGIN TRANSACTION;
EXECUTE  @ReturnCode = msdb.dbo.sp_add_job 
         @job_name = N'MDOP_DataCollector'
        ,@enabled = 1
        ,@notify_level_eventlog = 2
        ,@notify_level_email = 0
        ,@notify_level_netsend = 0
        ,@notify_level_page = 0
        ,@delete_level = 0
        ,@description = N'This job collects syscommittab and DRS data for testing the impact of various MDOP settings.'
        ,@category_name = N'Data Collector'
        ,@owner_login_name = N'sa'
        ,@job_id = @JobId OUTPUT;
IF (@@ERROR != 0 OR @ReturnCode != 0)
GOTO QuitWithRollback;
EXECUTE  @ReturnCode = msdb.dbo.sp_add_jobstep
         @job_id = @JobID
        ,@step_name = N'Collect & Store Info'
        ,@step_id = 1
        ,@cmdexec_success_code = 0
        ,@on_success_action = 1
        ,@on_success_step_id = 0
        ,@on_fail_action = 2 -- 2=Quit the job reporting failure; 1=Quit the job reporting success
        ,@on_fail_step_id = 0
        ,@retry_attempts = 0
        ,@retry_interval = 1
        ,@subsystem = N'TSQL'
        ,@command = N'
DECLARE  @GlobalCount int
        ,@SiteCount int
        ,@OutgoingCount int
        ,@OutdatedDrsCount int
        ,@MDOPSetting tinyint
        ,@MDOPCostThreshold smallint
        ,@CMDB sysname
        ,@BatchId int;
DECLARE @SysCommitInfoTmp table (name sysname NOT NULL,rows char(20),reserved varchar(18),data varchar(18),index_size varchar(18),unused varchar(18));

SELECT TOP 1 @CMDB = name
  FROM sys.databases
 WHERE name LIKE N''CM[_]___'';

-- This ensures we only run the data collection on the Primary node in an AG (if it is in one)
IF (
    SELECT ISNULL(ags.is_primary_replica,1)
      FROM sys.databases dbs
           LEFT OUTER JOIN (
                            SELECT adc.database_name,drs.is_local,is_primary_replica
                              FROM sys.availability_databases_cluster adc
                                   INNER JOIN sys.dm_hadr_database_replica_states drs
                                      ON adc.group_database_id = drs.group_database_id
                                     --AND drs.is_primary_replica = 0
                                     AND drs.database_state = 0
                                     AND drs.is_local = 1
                                   INNER JOIN sys.availability_replicas arp
                                      ON drs.replica_id = arp.replica_id
                                     AND arp.replica_server_name = @@SERVERNAME
                            ) ags
              ON dbs.name = ags.database_name
     WHERE dbs.name = @CMDB
       AND dbs.state = 0
    ) = 1
BEGIN
    -- Get the current MDOP setting:
    SELECT @MDOPSetting = CONVERT(tinyint,value_in_use)
      FROM sys.configurations
     WHERE name = N''max degree of parallelism'';

    SELECT @MDOPCostThreshold = CONVERT(smallint,value_in_use)
      FROM sys.configurations
     WHERE name = N''cost threshold for parallelism'';
    
    -- Get info for Drs Dbs:
    IF EXISTS (
    SELECT *
      FROM sys.change_tracking_databases ctd
           INNER JOIN sys.databases dbs
              ON ctd.database_id = dbs.database_id
             AND dbs.name = @CMDB
    )
    BEGIN
        SELECT  @GlobalCount = dbo.fnFastRowCount(N''ConfigMgrDrsQueue'')
               ,@SiteCount = dbo.fnFastRowCount(N''ConfigMgrDrsSiteQueue'');
        SELECT @OutgoingCount = COUNT(1)
          FROM sys.transmission_queue;
        SELECT @OutdatedDrsCount = COUNT(1)
          FROM sys.dm_tran_commit_table
         WHERE commit_time < DATEADD(day,-5,GETDATE());

        INSERT @SysCommitInfoTmp
        EXECUTE sp_spaceused ''sys.syscommittab'';

        SELECT @BatchId = ISNULL(MAX(BatchId),0)+1
          FROM MDOP_Test.dbo.DRSCountInfo;
        
        INSERT MDOP_Test.dbo.DRSCountInfo (BatchId,GlobalQueueCount,SiteQueueCount,OutgoingQueueCount,OutdatedDrsCount,MDOPSetting)
        VALUES (@BatchId,@GlobalCount,@SiteCount,@OutgoingCount,@OutdatedDrsCount,@MDOPSetting);

        SELECT @BatchId = ISNULL(MAX(BatchId),0)+1
          FROM MDOP_Test.dbo.SysCommitInfo;

        INSERT MDOP_Test.dbo.SysCommitInfo (BatchId,rows,reserved,data,index_size,unused,MDOPSetting)
        SELECT @BatchId,rows,reserved,data,index_size,unused,@MDOPSetting
          FROM @SysCommitInfoTmp;

        SELECT @BatchId = ISNULL(MAX(BatchId),0)+1
          FROM MDOP_Test.dbo.DrsReplGroupDelay;

        INSERT MDOP_Test.dbo.DrsReplGroupDelay (BatchId,CurrentSite,TargetSite,ReplicationGroup,SecondsOld,SumChangeCount,SumMessageCount,MDOPSetting)
        SELECT  @BatchId
               ,dbo.fnGetSiteCode() AS [CurrentSite]
               ,snd.TargetSite
               ,rep.ReplicationGroup
               ,DATEDIFF(second,MAX(snd.EndTime),GETUTCDATE()) AS [SecondsOld]
               ,MAX(old.SumChangeCount) AS [SumChangeCount]
               ,MAX(old.SumMessageCount) AS [SumMessageCount]
               ,@MDOPSetting
          FROM dbo.DrsSendHistory snd
               INNER JOIN dbo.ReplicationData rep
                  ON snd.ReplicationGroupID = rep.ID
               INNER JOIN (
                           SELECT  his.TargetSite
                                  ,his.ReplicationGroupID
                                  ,SUM(his.ChangeCount) AS [SumChangeCount]
                                  ,SUM(his.MessageCount) AS [SumMessageCount]
                             FROM dbo.DrsSendHistory his
                            WHERE his.ProcessedTime IS NULL
                            GROUP BY  his.TargetSite
                                     ,his.ReplicationGroupID
                           ) old
                  ON snd.TargetSite = old.TargetSite
                 AND snd.ReplicationGroupID = old.ReplicationGroupID
         WHERE snd.ProcessedTime IS NOT NULL
         GROUP BY  snd.TargetSite
                  ,rep.ReplicationGroup;
    END;

    -- Get DMV info for all Dbs/Servers:

    SELECT @BatchId = ISNULL(MAX(BatchId),0)+1
      FROM MDOP_Test.dbo.OsSchedulers;

    INSERT MDOP_Test.dbo.OsSchedulers (BatchId,scheduler_id,cpu_id,status,is_online,is_idle,parent_node_id,current_tasks_count,runnable_tasks_count,current_workers_count,active_workers_count,work_queue_count,pending_disk_io_count,preemptive_switches_count,context_switches_count,yield_count,load_factor,total_scheduler_delay_ms,MDOPSetting)
    SELECT @BatchId,scheduler_id,cpu_id,status,is_online,is_idle,parent_node_id,current_tasks_count,runnable_tasks_count,current_workers_count,active_workers_count,work_queue_count,pending_disk_io_count,preemptive_switches_count,context_switches_count,yield_count,load_factor,total_scheduler_delay_ms,@MDOPSetting
      FROM sys.dm_os_schedulers;

    SELECT @BatchId = ISNULL(MAX(BatchId),0)+1
      FROM MDOP_Test.dbo.OsSpinlockStats;

    INSERT MDOP_Test.dbo.OsSpinlockStats (BatchId,name,collisions,spins,spins_per_collision,sleep_time,backoffs,MDOPSetting)
    SELECT @BatchId,name,collisions,spins,spins_per_collision,sleep_time,backoffs,@MDOPSetting
      FROM sys.dm_os_spinlock_stats
     WHERE collisions > 0;

    SELECT @BatchId = ISNULL(MAX(BatchId),0)+1
      FROM MDOP_Test.dbo.OsWaitStats;

    INSERT MDOP_Test.dbo.OsWaitStats (BatchId,wait_type,waiting_tasks_count,wait_time_ms,max_wait_time_ms,signal_wait_time_ms,MDOPSetting)
    SELECT @BatchId,wait_type,waiting_tasks_count,wait_time_ms,max_wait_time_ms,signal_wait_time_ms,@MDOPSetting
      FROM sys.dm_os_wait_stats;

    SELECT @BatchId = ISNULL(MAX(BatchId),0)+1
      FROM MDOP_Test.dbo.OsWaitingTasks;

    INSERT MDOP_Test.dbo.OsWaitingTasks (BatchId,waiting_task_address,session_id,exec_context_id,wait_duration_ms,wait_type,resource_address,blocking_task_address,blocking_session_id,blocking_exec_context_id,resource_description,MDOPSetting)
    SELECT @BatchId,waiting_task_address,session_id,exec_context_id,wait_duration_ms,wait_type,resource_address,blocking_task_address,blocking_session_id,blocking_exec_context_id,resource_description,@MDOPSetting
      FROM sys.dm_os_waiting_tasks;
END;
'
        ,@database_name = @CMDB
        ,@flags = 0;
IF (@@ERROR != 0 OR @ReturnCode != 0)
GOTO QuitWithRollback;
EXECUTE  @ReturnCode = msdb.dbo.sp_update_job
         @job_id = @JobID
        ,@start_step_id = 1;
IF (@@ERROR != 0 OR @ReturnCode != 0)
GOTO QuitWithRollback;
EXECUTE  @ReturnCode = msdb.dbo.sp_add_jobschedule
         @job_id = @JobID
        ,@name = N'Every 5 minutes'
        ,@enabled = 1
        ,@freq_type = 4
        ,@active_start_date = 20011026
        ,@active_start_time = 0
        ,@freq_interval = 1
        ,@freq_subday_type = 4
        ,@freq_subday_interval = 5
        ,@freq_relative_interval= 0
        ,@freq_recurrence_factor = 0
        ,@active_end_date = 99991231
        ,@active_end_time = 235959
IF (@@ERROR != 0 OR @ReturnCode != 0)
GOTO QuitWithRollback;
EXECUTE @ReturnCode = msdb.dbo.sp_add_jobserver
        @job_id = @JobId
       ,@server_name = N'(local)';
IF (@@ERROR != 0 OR @ReturnCode != 0)
GOTO QuitWithRollback;
COMMIT TRANSACTION;
PRINT 'Job Created.';
GOTO EndScript;

QuitWithRollback:
IF (@@TRANCOUNT > 0)
ROLLBACK TRANSACTION;
PRINT N'Job Not created!';
GOTO EndScript;

DBNotCreated:
PRINT N'';
PRINT N'There were some errors or script issues encountered; please see any previous messages for details.';
GOTO EndScript;

EndScript:
GO


/**************************************************
----   CLEANUP SCRIPT   ----
USE [msdb];
IF EXISTS (SELECT * FROM msdb.dbo.sysjobs_view WHERE name = N'MDOP_DataCollector')
EXECUTE msdb.dbo.sp_delete_job @job_name = N'MDOP_DataCollector', @delete_unused_schedule = 1;

USE [master];
DROP DATABASE IF EXISTS MDOP_Test;
**************************************************/
