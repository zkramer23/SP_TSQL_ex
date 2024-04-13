CREATE PROCEDURE sp_LoadAndMergeMultipleDataFiles
    @FilePath1 NVARCHAR(1000), -- Full path to the first flat file
    @FilePath2 NVARCHAR(1000)  -- Full path to the second flat file
AS
BEGIN
    -- Setup error handling
    BEGIN TRY
        -- Create the first temporary table and load data
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

        -- Merge data into the first destination table 'pbl1'
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

        -- Create the second temporary table and load data
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

        -- Merge data into the second destination table 'pbl2'
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

        -- Commit the transaction to confirm changes
        COMMIT;
    END TRY
    BEGIN CATCH
        -- Rollback transaction on error and log the error
        ROLLBACK;
        -- Log error with details about which part of the process failed
        DECLARE @ErrorID1 INT = dbo.LogError('sp_LoadAndMergeMultipleDataFiles', 'Bulk Insert 1');
        DECLARE @ErrorID2 INT = dbo.LogError('sp_LoadAndMergeMultipleDataFiles', 'Bulk Insert 2');
        -- Rethrow the error for external handling
        RAISERROR (ERROR_MESSAGE(), ERROR_SEVERITY(), ERROR_STATE());
    END CATCH
END;
