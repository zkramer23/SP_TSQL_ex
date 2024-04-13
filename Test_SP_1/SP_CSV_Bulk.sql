CREATE PROCEDURE sp_LoadAndMergeMultipleDataFiles
    @FilePath1 NVARCHAR(1000), -- Full path to the first flat file
    @FilePath2 NVARCHAR(1000)  -- Full path to the second flat file
AS
BEGIN
    -- Begin the overall transaction (optional, depending on need for atomicity across all operations)
    BEGIN TRANSACTION;

    -- First bulk insert and merge operation
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
    END TRY
    BEGIN CATCH
        -- Log error with details about the first process failure
        DECLARE @ErrorID1 INT = dbo.LogError('sp_LoadAndMergeMultipleDataFiles', 'Bulk Insert 1');
    END CATCH

    -- Second bulk insert and merge operation
    BEGIN TRY
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
    END TRY
    BEGIN CATCH
        -- Log error with details about the second process failure
        DECLARE @ErrorID2 INT = dbo.LogError('sp_LoadAndMergeMultipleDataFiles', 'Bulk Insert 2');
    END CATCH

    -- Decide to commit or rollback based on the presence of errors
    IF @@TRANCOUNT > 0
        COMMIT TRANSACTION;
END;
