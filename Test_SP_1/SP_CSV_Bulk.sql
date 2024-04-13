CREATE PROCEDURE sp_LoadAndMergeMultipleDataFiles
    @FilePath1 NVARCHAR(1000), -- Full path to the first flat file
    @FilePath2 NVARCHAR(1000)  -- Full path to the second flat file
AS
BEGIN
    DECLARE @RowsInserted1 INT, @RowsUpdated1 INT;
    DECLARE @RowsInserted2 INT, @RowsUpdated2 INT;
    DECLARE @UserName NVARCHAR(255) = SYSTEM_USER;

    -- Handling each file process in separate transactions
    -- First file process
    BEGIN TRANSACTION;
    BEGIN TRY
        DROP TABLE IF EXISTS #TempData1;
        CREATE TABLE #TempData1 (
            ID INT,
            Name NVARCHAR(255),
            Position NVARCHAR(255)
        );

        BULK INSERT #TempData1
        FROM @FilePath1
        WITH (
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n',  
            FIRSTROW = 2          
        );

        MERGE INTO pbl1 AS Target
        USING #TempData1 AS Source
            ON Target.ID = Source.ID
        WHEN MATCHED THEN
            UPDATE SET
                Target.Name = Source.Name,
                Target.Position = Source.Position
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (ID, Name, Position)
            VALUES (Source.ID, Source.Name, Source.Position);

        -- Logging the number of inserted and updated rows immediately
        SET @RowsInserted1 = @@ROWCOUNT;  -- Assuming this counts total affected, needs to differentiate by $action if possible
        SET @RowsUpdated1 = 0;            -- Similarly update this value based on actual action if possible

        INSERT INTO MergeLog (ProcedureName, UserName, RunDateTime, RowsInserted, RowsUpdated, RowsDeleted)
        VALUES ('sp_LoadAndMergeMultipleDataFiles', @UserName, GETDATE(), @RowsInserted1, @RowsUpdated1, 0);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        INSERT INTO ErrorLog (ProcedureName, UserName, ErrorDateTime, ErrorMessage, ErrorSeverity, ErrorState)
        VALUES ('sp_LoadAndMergeMultipleDataFiles', @UserName, GETDATE(), ERROR_MESSAGE(), ERROR_SEVERITY(), ERROR_STATE());
    END CATCH

    -- Second file process
    BEGIN TRANSACTION;
    BEGIN TRY
        DROP TABLE IF EXISTS #TempData2;
        CREATE TABLE #TempData2 (
            EmployeeID INT,
            Department NVARCHAR(255),
            Salary INT
        );

        BULK INSERT #TempData2
        FROM @FilePath2
        WITH (
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n',  
            FIRSTROW = 2          
        );

        MERGE INTO pbl2 AS Target
        USING #TempData2 AS Source
            ON Target.EmployeeID = Source.EmployeeID
        WHEN MATCHED THEN
            UPDATE SET
                Target.Department = Source.Department,
                Target.Salary = Source.Salary
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (EmployeeID, Department, Salary)
            VALUES (Source.EmployeeID, Source.Department, Source.Salary);

        SET @RowsInserted2 = @@ROWCOUNT;  -- Assuming this counts total affected, needs to differentiate by $action if possible
        SET @RowsUpdated2 = 0;            -- Similarly update this value based on actual action if possible

        INSERT INTO MergeLog (ProcedureName, UserName, RunDateTime, RowsInserted, RowsUpdated, RowsDeleted)
        VALUES ('sp_LoadAndMergeMultipleDataFiles', @UserName, GETDATE(), @RowsInserted2, @RowsUpdated2, 0);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        INSERT INTO ErrorLog (ProcedureName, UserName, ErrorDateTime, ErrorMessage, ErrorSeverity, ErrorState)
        VALUES ('sp_LoadAndMergeMultipleDataFiles', @UserName, GETDATE(), ERROR_MESSAGE(), ERROR_SEVERITY(), ERROR_STATE());
    END CATCH
END;
