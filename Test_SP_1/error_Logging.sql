



CREATE TABLE ErrorLog (
    ErrorLogID INT IDENTITY(1,1) PRIMARY KEY,
    ErrorOperation NVARCHAR(200),
    ErrorStep NVARCHAR(200),
    ErrorTime DATETIME,
    ErrorMessage NVARCHAR(4000),
    ErrorSeverity INT,
    ErrorState INT
);


CREATE TABLE MergeLog (
    MergeLogID INT IDENTITY(1,1) PRIMARY KEY,
    ProcedureName NVARCHAR(200),
    UserName NVARCHAR(255),
    RunDateTime DATETIME,
    RowsInserted INT,
    RowsUpdated INT,
    RowsDeleted INT
);


CREATE FUNCTION dbo.LogError 
(
    @ErrorOperation NVARCHAR(200),
    @ErrorStep NVARCHAR(200)
)
RETURNS INT
AS
BEGIN
    DECLARE @ErrorID INT;

    INSERT INTO ErrorLog (ErrorOperation, ErrorStep, ErrorTime, ErrorMessage, ErrorSeverity, ErrorState)
    VALUES (@ErrorOperation, @ErrorStep, GETDATE(), ERROR_MESSAGE(), ERROR_SEVERITY(), ERROR_STATE());

    SET @ErrorID = SCOPE_IDENTITY(); -- Get the ID of the inserted error record

    RETURN @ErrorID; -- Optionally return the ErrorLog ID for further use
END
