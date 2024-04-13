



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

