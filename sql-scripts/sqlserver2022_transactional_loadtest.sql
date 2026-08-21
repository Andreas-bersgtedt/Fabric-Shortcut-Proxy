/*
    SQL Server 2022 transactional load-test dataset.

    This script is intentionally standalone and is not used by the application.
    It creates three tables with identical transactional shapes:
      scaletest.TransactionLarge - 10 million row target
      scaletest.TransactionXL    - 100 million row target
      scaletest.TransactionXXL   - 1 billion row target

    The procedures process rows in batches. No initial load runs automatically.
    Run the examples at the bottom explicitly when the test environment is ready.
*/

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET XACT_ABORT ON;
GO

IF SCHEMA_ID(N'scaletest') IS NULL
    EXEC(N'CREATE SCHEMA scaletest AUTHORIZATION dbo;');
GO

IF OBJECT_ID(N'scaletest.TransactionLarge', N'U') IS NULL
BEGIN
    CREATE TABLE scaletest.TransactionLarge
    (
        TransactionId bigint IDENTITY(1, 1) NOT NULL,
        BusinessKey bigint NOT NULL,
        AccountId int NOT NULL,
        ProductId int NOT NULL,
        RegionId smallint NOT NULL,
        Quantity int NOT NULL,
        UnitPrice decimal(19, 4) NOT NULL,
        TransactionAmount decimal(19, 4) NOT NULL,
        TransactionStatus tinyint NOT NULL,
        CreatedDateTime datetime2(3) NOT NULL,
        LastModifiedDateTime datetime2(3) NOT NULL,
        CONSTRAINT PK_TransactionLarge PRIMARY KEY CLUSTERED (TransactionId),
        CONSTRAINT UQ_TransactionLarge_BusinessKey UNIQUE NONCLUSTERED (BusinessKey)
    );
END;
GO

IF OBJECT_ID(N'scaletest.TransactionXL', N'U') IS NULL
BEGIN
    CREATE TABLE scaletest.TransactionXL
    (
        TransactionId bigint IDENTITY(1, 1) NOT NULL,
        BusinessKey bigint NOT NULL,
        AccountId int NOT NULL,
        ProductId int NOT NULL,
        RegionId smallint NOT NULL,
        Quantity int NOT NULL,
        UnitPrice decimal(19, 4) NOT NULL,
        TransactionAmount decimal(19, 4) NOT NULL,
        TransactionStatus tinyint NOT NULL,
        CreatedDateTime datetime2(3) NOT NULL,
        LastModifiedDateTime datetime2(3) NOT NULL,
        CONSTRAINT PK_TransactionXL PRIMARY KEY CLUSTERED (TransactionId),
        CONSTRAINT UQ_TransactionXL_BusinessKey UNIQUE NONCLUSTERED (BusinessKey)
    );
END;
GO

IF OBJECT_ID(N'scaletest.TransactionXXL', N'U') IS NULL
BEGIN
    CREATE TABLE scaletest.TransactionXXL
    (
        TransactionId bigint IDENTITY(1, 1) NOT NULL,
        BusinessKey bigint NOT NULL,
        AccountId int NOT NULL,
        ProductId int NOT NULL,
        RegionId smallint NOT NULL,
        Quantity int NOT NULL,
        UnitPrice decimal(19, 4) NOT NULL,
        TransactionAmount decimal(19, 4) NOT NULL,
        TransactionStatus tinyint NOT NULL,
        CreatedDateTime datetime2(3) NOT NULL,
        LastModifiedDateTime datetime2(3) NOT NULL,
        CONSTRAINT PK_TransactionXXL PRIMARY KEY CLUSTERED (TransactionId),
        CONSTRAINT UQ_TransactionXXL_BusinessKey UNIQUE NONCLUSTERED (BusinessKey)
    );
END;
GO

/* Keep the clustered rowstore key for range materialization; add columnstore for analytics. */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'CCI_TransactionLarge' AND object_id = OBJECT_ID(N'scaletest.TransactionLarge'))
    CREATE NONCLUSTERED COLUMNSTORE INDEX CCI_TransactionLarge ON scaletest.TransactionLarge;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'CCI_TransactionXL' AND object_id = OBJECT_ID(N'scaletest.TransactionXL'))
    CREATE NONCLUSTERED COLUMNSTORE INDEX CCI_TransactionXL ON scaletest.TransactionXL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'CCI_TransactionXXL' AND object_id = OBJECT_ID(N'scaletest.TransactionXXL'))
    CREATE NONCLUSTERED COLUMNSTORE INDEX CCI_TransactionXXL ON scaletest.TransactionXXL;
GO

CREATE OR ALTER PROCEDURE scaletest.InsertTransactions
    @TableSize varchar(5),
    @Rows bigint,
    @BatchSize int = 1000000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @TableSize NOT IN ('large', 'xl', 'xxl') OR @Rows < 1 OR @BatchSize < 1
        THROW 51000, 'TableSize must be large, xl, or xxl; Rows and BatchSize must be positive.', 1;

    DECLARE @Target nvarchar(300) = N'scaletest.' + CASE @TableSize
        WHEN 'large' THEN N'TransactionLarge'
        WHEN 'xl' THEN N'TransactionXL'
        ELSE N'TransactionXXL' END;
    DECLARE @Sql nvarchar(max) = N'
        DECLARE @BatchStart bigint = 0;
        WHILE @BatchStart < @Rows
        BEGIN
            DECLARE @ThisBatch int = CONVERT(int, IIF(@Rows - @BatchStart > @BatchSize, @BatchSize, @Rows - @BatchStart));
            DECLARE @Now datetime2(3) = SYSUTCDATETIME();
            ;WITH Numbers AS
            (
                SELECT TOP (@ThisBatch) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
                FROM sys.all_objects a CROSS JOIN sys.all_objects b
            )
            INSERT INTO ' + @Target + N'
                (BusinessKey, AccountId, ProductId, RegionId, Quantity, UnitPrice, TransactionAmount, TransactionStatus, CreatedDateTime, LastModifiedDateTime)
            SELECT @BatchStart + n + 1, 1 + CONVERT(int, (@BatchStart + n) % 1000000),
                1 + CONVERT(int, (@BatchStart + n) % 10000), 1 + CONVERT(smallint, (@BatchStart + n) % 100),
                1 + CONVERT(int, (@BatchStart + n) % 20),
                CONVERT(decimal(19, 4), 5.00 + ((@BatchStart + n) % 100000) / 100.0),
                CONVERT(decimal(19, 4), (1 + CONVERT(int, (@BatchStart + n) % 20)) * (5.00 + ((@BatchStart + n) % 100000) / 100.0)),
                CONVERT(tinyint, (@BatchStart + n) % 5),
                DATEADD(month, -CONVERT(int, (@BatchStart + n) % 12), @Now),
                DATEADD(month, -CONVERT(int, (@BatchStart + n) % 12), @Now)
            FROM Numbers;
            SET @BatchStart += @ThisBatch;
        END;';
    EXEC sys.sp_executesql @Sql, N'@Rows bigint, @BatchSize int', @Rows, @BatchSize;
END;
GO

CREATE OR ALTER PROCEDURE scaletest.UpdateTransactions
    @TableSize varchar(5),
    @Rows bigint,
    @BatchSize int = 1000000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @TableSize NOT IN ('large', 'xl', 'xxl') OR @Rows < 1 OR @BatchSize < 1
        THROW 51001, 'TableSize must be large, xl, or xxl; Rows and BatchSize must be positive.', 1;
    DECLARE @Target nvarchar(300) = N'scaletest.' + CASE @TableSize WHEN 'large' THEN N'TransactionLarge' WHEN 'xl' THEN N'TransactionXL' ELSE N'TransactionXXL' END;
    DECLARE @Sql nvarchar(max) = N'
        DECLARE @Remaining bigint = @Rows;
        WHILE @Remaining > 0
        BEGIN
            DECLARE @ThisBatch int = CONVERT(int, IIF(@Remaining > @BatchSize, @BatchSize, @Remaining));
            UPDATE TOP (@ThisBatch) ' + @Target + N' WITH (TABLOCKX)
                SET Quantity = Quantity + 1, TransactionAmount = (Quantity + 1) * UnitPrice,
                    TransactionStatus = (TransactionStatus + 1) % 5, LastModifiedDateTime = SYSUTCDATETIME();
            IF @@ROWCOUNT = 0 BREAK;
            SET @Remaining -= @@ROWCOUNT;
        END;';
    EXEC sys.sp_executesql @Sql, N'@Rows bigint, @BatchSize int', @Rows, @BatchSize;
END;
GO

CREATE OR ALTER PROCEDURE scaletest.UpsertTransactions
    @TableSize varchar(5),
    @Rows bigint,
    @BatchSize int = 1000000
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    IF @TableSize NOT IN ('large', 'xl', 'xxl') OR @Rows < 1 OR @BatchSize < 1
        THROW 51002, 'TableSize must be large, xl, or xxl; Rows and BatchSize must be positive.', 1;
    DECLARE @Target nvarchar(300) = N'scaletest.' + CASE @TableSize WHEN 'large' THEN N'TransactionLarge' WHEN 'xl' THEN N'TransactionXL' ELSE N'TransactionXXL' END;
    DECLARE @Sql nvarchar(max) = N'
        DECLARE @BatchStart bigint = 0;
        WHILE @BatchStart < @Rows
        BEGIN
            DECLARE @ThisBatch int = CONVERT(int, IIF(@Rows - @BatchStart > @BatchSize, @BatchSize, @Rows - @BatchStart));
            DECLARE @Now datetime2(3) = SYSUTCDATETIME();
            BEGIN TRANSACTION;
            ;WITH Numbers AS (SELECT TOP (@ThisBatch) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
            UPDATE target WITH (UPDLOCK, HOLDLOCK)
                SET Quantity = target.Quantity + 1, LastModifiedDateTime = @Now
            FROM ' + @Target + N' target INNER JOIN Numbers ON target.BusinessKey = @BatchStart + Numbers.n + 1;
            ;WITH Numbers AS (SELECT TOP (@ThisBatch) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
            INSERT INTO ' + @Target + N' (BusinessKey, AccountId, ProductId, RegionId, Quantity, UnitPrice, TransactionAmount, TransactionStatus, CreatedDateTime, LastModifiedDateTime)
            SELECT @BatchStart + n + 1, 1 + CONVERT(int, (@BatchStart + n) % 1000000), 1 + CONVERT(int, (@BatchStart + n) % 10000), 1 + CONVERT(smallint, (@BatchStart + n) % 100), 1, 10.00, 10.00, 0, @Now, @Now
            FROM Numbers WHERE NOT EXISTS (SELECT 1 FROM ' + @Target + N' existing WITH (UPDLOCK, HOLDLOCK) WHERE existing.BusinessKey = @BatchStart + Numbers.n + 1);
            COMMIT TRANSACTION;
            SET @BatchStart += @ThisBatch;
        END;';
    EXEC sys.sp_executesql @Sql, N'@Rows bigint, @BatchSize int', @Rows, @BatchSize;
END;
GO

/* Initial-load examples. Execute one at a time in a dedicated load-test database. */
-- EXEC scaletest.InsertTransactions 'large', 10000000;
-- EXEC scaletest.InsertTransactions 'xl',    100000000;
-- EXEC scaletest.InsertTransactions 'xxl',   1000000000;
-- EXEC scaletest.UpdateTransactions 'large', 1000000;
-- EXEC scaletest.UpsertTransactions 'large', 1000000;