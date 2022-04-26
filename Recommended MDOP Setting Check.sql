/********************************************************************************
Logic for this script comes from https://support.microsoft.com/en-us/help/2806535/recommendations-and-guidelines-for-the-max-degree-of-parallelism-confi
********************************************************************************/
SET NOCOUNT ON;

DECLARE  @CurMDOP smallint
        ,@RecMDOP smallint
        ,@SQLMajorVersion tinyint
        ,@NumaNodes int
        ,@ProcsPerNumaNode int
        ,@IsWithinRecommendation bit
        ,@Output varchar(max);

SELECT @CurMDOP = CONVERT(smallint,value_in_use) FROM sys.configurations WHERE name = N'max degree of parallelism';
SELECT @SQLMajorVersion = CONVERT(tinyint,LEFT(CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion')),CHARINDEX(N'.',CONVERT(nvarchar(128),SERVERPROPERTY('ProductVersion')))-1));

SELECT  @NumaNodes = COUNT(NumaNodeId)
       ,@ProcsPerNumaNode = MIN(LogicalProcsPerNumaNode)
  FROM (
        SELECT  parent_node_id AS [NumaNodeId]
               ,COUNT(DISTINCT cpu_id) AS [LogicalProcsPerNumaNode]
          FROM sys.dm_os_schedulers
         WHERE status = N'VISIBLE ONLINE'
         GROUP BY parent_node_id
        ) dta;

IF @SQLMajorVersion >= 13
BEGIN
    SELECT @RecMDOP = CASE WHEN @NumaNodes = 1 THEN CASE WHEN @ProcsPerNumaNode <= 8 THEN @ProcsPerNumaNode ELSE 8 END
                           ELSE CASE WHEN @ProcsPerNumaNode <= 16 THEN @ProcsPerNumaNode
                                     ELSE CASE WHEN @ProcsPerNumaNode*0.5 <= 16 THEN @ProcsPerNumaNode*0.5 ELSE 16 END
                                END
                      END;
    IF @NumaNodes = 1 AND @ProcsPerNumaNode <= 8 AND @CurMDOP <= @ProcsPerNumaNode SET @IsWithinRecommendation = 1;
    IF @NumaNodes > 1 AND @ProcsPerNumaNode <= 16 AND @CurMDOP <= @ProcsPerNumaNode SET @IsWithinRecommendation = 1;
END;
ELSE
BEGIN
    SELECT @RecMDOP = CASE WHEN @ProcsPerNumaNode < 8 THEN @ProcsPerNumaNode ELSE 8 END;
    IF @ProcsPerNumaNode <= 8 AND @CurMDOP <= @ProcsPerNumaNode SET @IsWithinRecommendation = 1;
END;

IF @CurMDOP != @RecMDOP
BEGIN
    SELECT @Output = CASE WHEN @IsWithinRecommendation = 1 THEN '-- MDOP is within the recommended setting and MAY benefit from setting it differently.
-- The Max Recommended MDOP setting is '+CONVERT(varchar(2),@RecMDOP)+' and the server is currently configured to '+CONVERT(varchar(2),@CurMDOP)+'.
-- Consider running the following to set the server to the max recommended setting:
'
                          ELSE '-- MDOP NOT AT RECOMMENDED SETTING!
-- Recommended MDOP setting is '+CONVERT(varchar(2),@RecMDOP)+' and the server is currently configured to '+CONVERT(varchar(2),@CurMDOP)+'.
-- Run the following to set the server to the recommended setting:
'
                     END;
    SELECT @Output += 'DECLARE @AdvancedOptions bit;
SELECT @AdvancedOptions = CONVERT(bit,value_in_use)
  FROM sys.configurations
 WHERE name = N''show advanced options'';
IF @AdvancedOptions = 0
BEGIN
    EXECUTE sp_configure ''show advanced options'', 1;
    RECONFIGURE WITH OVERRIDE;
END;
EXECUTE sp_configure ''max degree of parallelism'', '+CONVERT(varchar(2),@RecMDOP)+';
RECONFIGURE WITH OVERRIDE;
IF @AdvancedOptions = 0
BEGIN
    EXECUTE sp_configure ''show advanced options'', 0;
    RECONFIGURE WITH OVERRIDE;
END;
GO';
    
    PRINT @Output;
END;
ELSE
BEGIN
    PRINT '-- MDOP configured at the recommended setting!
-- Recommended MDOP setting is '+CONVERT(varchar(2),@RecMDOP)+' and the server is currently configured to '+CONVERT(varchar(2),@CurMDOP)+'.';
END;
GO
