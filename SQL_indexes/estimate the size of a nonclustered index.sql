/*
estimate the size of a nonclustered index v.3.2
https://docs.microsoft.com/en-us/sql/relational-databases/databases/estimate-the-size-of-a-nonclustered-index?view=sql-server-2016
Valmar Moritz
Dec 25th, 2019
*/

SET NOCOUNT ON
SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

/* MANUAL INPUTS */

USE []    -- database name

DECLARE @schema varchar(128) = 'dbo'     -- schema name
DECLARE @table  varchar(128) = ''     -- table name

DECLARE @indexName varchar(128) = ''     -- name of the index if already existing

DECLARE @keyCol1   varchar(128) = ''     -- columns in index key
DECLARE @keyCol2   varchar(128) = ''
DECLARE @keyCol3   varchar(128) = ''
DECLARE @keyCol4   varchar(128) = ''
DECLARE @keyCol5   varchar(128) = ''
DECLARE @keyCol6   varchar(128) = ''
DECLARE @keyCol7   varchar(128) = ''
DECLARE @keyCol8   varchar(128) = ''
DECLARE @keyCol9   varchar(128) = ''

DECLARE @incCol1   varchar(128) = ''     -- included columns
DECLARE @incCol2   varchar(128) = ''
DECLARE @incCol3   varchar(128) = ''
DECLARE @incCol4   varchar(128) = ''
DECLARE @incCol5   varchar(128) = ''
DECLARE @incCol6   varchar(128) = ''
DECLARE @incCol7   varchar(128) = ''
DECLARE @incCol8   varchar(128) = ''
DECLARE @incCol9   varchar(128) = ''

/* END OF MANUAL INPUTS */



/* prepare to run queries */
DECLARE @tableToIndex varchar(392) = QUOTENAME(@schema) + '.' + QUOTENAME(@table)

DECLARE @columnsToIndex table (
	[Column Name] nvarchar(128)
	,keyColumn int
	,incColumn int
	,[Data type] nvarchar(128)
	,[Max Byte Length] int
	,isVariableLength bit
	,[isNullable] bit
	)

INSERT INTO @columnsToIndex ([Column Name], keyColumn, incColumn)
SELECT kc, 1, NULL
FROM (VALUES (@keyCol1), (@keyCol2), (@keyCol3), (@keyCol4), (@keyCol5), (@keyCol6), (@keyCol7), (@keyCol8), (@keyCol9)) v (kc) WHERE kc <> ''
UNION
SELECT ic, NULL, 1
FROM (VALUES (@incCol1), (@incCol2), (@incCol3), (@incCol4), (@incCol5), (@incCol6), (@incCol7), (@incCol8), (@incCol9)) v (ic) WHERE ic <> ''

/* in the following the data types are categorized as variable-length or fixed-length as per https://www.guru99.com/sql-server-datatype.html */
UPDATE ci
SET 
	ci.[Data type] = t.[name]
	,[Max Byte Length] = c.max_length
	,ci.isNullable = c.is_nullable
	,isVariableLength = CASE WHEN c.user_type_id IN (
		127 -- bigint
		,56 -- int
		,52 -- smallint
		,48 -- tinyint
		,104 -- bit
		,60 -- money
		,122 -- smallmoney
		,59 -- real
		,61 -- datetime
		,58 -- smalldatetime
		,40 -- date
		,41 -- time
		,43 -- datetimeoffset
		,42 -- datetime2
		,36 -- uniqueidentifier
		) THEN 0 ELSE 1 END
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
JOIN @columnsToIndex ci ON ci.[Column Name] = c.[Name]
WHERE c.object_id = OBJECT_ID(@tableToIndex)



--- what do we have?
SELECT * FROM @columnsToIndex
ORDER BY keyColumn DESC, incColumn DESC



/* BEGIN queries to calculate average data lengths  */
---
DECLARE @variableLengthStorageFullnessOfKeyColumns float = 1.0

IF EXISTS (SELECT 1 FROM @columnsToIndex WHERE isVariableLength = 1 AND keyColumn = 1)
BEGIN

	DECLARE @sql1 nvarchar(max) = ';WITH rd AS ('

	SET @sql1 += (
		SELECT (
			SELECT CAST(x.cmd_line as nvarchar(max))
			FROM (
				SELECT CASE WHEN ROW_NUMBER() OVER (ORDER BY [Column Name]) = 1 THEN '' ELSE ' UNION ALL ' END
					+ 'SELECT AVG(CAST(DATALENGTH(' + [Column Name] + ') as bigint)) AS AverageDataLengthForKeyColumn, '
					+ CONVERT(varchar(max),[Max Byte Length]) + ' AS MaxDataLengthForKeyColumn FROM ' + @tableToIndex + '' AS cmd_line
				FROM @columnsToIndex
				WHERE isVariableLength = 1
					AND keyColumn = 1
				) x
			FOR XML PATH ('')
			)
		)

	SET @sql1 += ') SELECT @variableLengthStorageFullnessOfKeyColumns = SUM(AverageDataLengthForKeyColumn) * 1.0 / SUM(MaxDataLengthForKeyColumn) FROM rd'

	EXEC sp_executesql @sql1
		,N'@variableLengthStorageFullnessOfKeyColumns float OUTPUT'
		,@variableLengthStorageFullnessOfKeyColumns = @variableLengthStorageFullnessOfKeyColumns OUTPUT

END

---
DECLARE @variableLengthStorageFullnessOfAllColumns float = 1.0

IF EXISTS (SELECT 1 FROM @columnsToIndex WHERE isVariableLength = 1)
BEGIN

	DECLARE @sql2 nvarchar(max) = ';WITH rd AS ('

	SET @sql2 += (
		SELECT (
			SELECT CAST(x.cmd_line as nvarchar(max))
			FROM (
				SELECT CASE WHEN ROW_NUMBER() OVER (ORDER BY [Column Name]) = 1 THEN '' ELSE ' UNION ALL ' END
					+ 'SELECT AVG(CAST(DATALENGTH(' + [Column Name] + ') as bigint)) AS AverageDataLengthForColumn, '
					+ CONVERT(varchar(max),[Max Byte Length]) + ' AS MaxDataLengthForColumn FROM ' + @tableToIndex + '' AS cmd_line
				FROM @columnsToIndex
				WHERE isVariableLength = 1
				) x
			FOR XML PATH ('')
			)
		)

	SET @sql2 += ') SELECT @variableLengthStorageFullnessOfAllColumns = SUM(AverageDataLengthForColumn) * 1.0 / SUM(MaxDataLengthForColumn) FROM rd'

	EXEC sp_executesql @sql2
		,N'@variableLengthStorageFullnessOfAllColumns float OUTPUT'
		,@variableLengthStorageFullnessOfAllColumns = @variableLengthStorageFullnessOfAllColumns OUTPUT

END
/* END queries to calculate average data lengths  */




---
SELECT @variableLengthStorageFullnessOfKeyColumns AS variableLengthStorageFullnessOfKeyColumns
	,@variableLengthStorageFullnessOfAllColumns AS variableLengthStorageFullnessOfAllColumns




/* Step 1. Calculate Variables for Use in Steps 2 and 3 */
DECLARE @Num_Rows bigint
DECLARE @sql nvarchar(max) = 'SELECT @Num_Rows = SUM(st.row_count) FROM sys.tables t JOIN sys.dm_db_partition_stats st ON t.object_id = st.object_id WHERE schema_name(t.schema_id) = ''' + @schema + ''' AND object_name(st.object_id) =  ''' + @table + ''' AND index_id < 2'
EXECUTE sp_executesql @sql, N'@Num_Rows bigint OUTPUT', @Num_Rows = @Num_Rows OUTPUT

DECLARE @Num_Key_Cols int = (SELECT COUNT(keyColumn) FROM @columnsToIndex)


DECLARE @Fixed_Key_Size int = (SELECT ISNULL(SUM([Max Byte Length]), 0) FROM @columnsToIndex WHERE isVariableLength = 0)

DECLARE @Num_Variable_Key_Cols int
DECLARE @Max_Var_Key_Size int

SELECT @Num_Variable_Key_Cols = COUNT(*)
	,@Max_Var_Key_Size = SUM([Max Byte Length])
FROM @columnsToIndex
WHERE isVariableLength = 1


/* let's assume the index is nonunique */
SET @Num_Key_Cols += 1
SET @Num_Variable_Key_Cols += 1
SET @Max_Var_Key_Size += 8



DECLARE @Index_Null_Bitmap int = 0

IF EXISTS (SELECT 1 FROM @columnsToIndex WHERE isNullable = 1)
SET @Index_Null_Bitmap = (SELECT 2 + ((COUNT(*) + 7) / 8) FROM @columnsToIndex)

DECLARE @Variable_Key_Size int = 2 + (@Num_Variable_Key_Cols * 2) + (@Max_Var_Key_Size * @variableLengthStorageFullnessOfKeyColumns)
DECLARE @Index_Row_Size int = @Fixed_Key_Size + @Variable_Key_Size + @Index_Null_Bitmap + 1 + 6
DECLARE @Index_Rows_Per_Page int = 8096 / (@Index_Row_Size + 2)


---
SELECT @Num_Rows AS Num_Rows
	,@Num_Key_Cols AS Num_Key_Columns
	,@Fixed_Key_Size AS Fixed_Key_Size
	,@Num_Variable_Key_Cols AS Num_Variable_Key_Cols
	,@Max_Var_Key_Size AS Max_Var_Key_Size
	,@Index_Null_Bitmap AS Index_Null_Bitmap
	,@variableLengthStorageFullnessOfKeyColumns AS StorageFullness
	,@Variable_Key_Size AS Variable_Key_Size
	,@Index_Row_Size AS Index_Row_Size
	,@Index_Rows_Per_Page AS Index_Rows_Per_Page





/* Step 2. Calculate the Space Used to Store Index Information in the Leaf Level */

/* add included columns size requirements, if necessary */
DECLARE @Num_Leaf_Cols int = @Num_Key_Cols + (SELECT COUNT(incColumn) FROM @columnsToIndex)
DECLARE @Fixed_Leaf_Size  int = @Fixed_Key_Size + (SELECT ISNULL(SUM([Max Byte Length]),0) FROM @columnsToIndex WHERE incColumn = 1)

DECLARE @Num_Variable_Leaf_Cols int
DECLARE @Max_Var_Leaf_Size int

SELECT @Num_Variable_Leaf_Cols = @Num_Variable_Key_Cols + COUNT(*)
	,@Max_Var_Leaf_Size = @Max_Var_Key_Size + ISNULL(SUM([Max Byte Length]),0)
FROM @columnsToIndex
WHERE incColumn = 1 AND isVariableLength = 1


/* as we assumed earlier that the index is nonunique, no additional reservations for data row locators needed here */

DECLARE @Leaf_Null_Bitmap int = 2 + ((@Num_Leaf_Cols + 7) / 8)
DECLARE @Variable_Leaf_Size int = 0

IF EXISTS (SELECT 1 FROM @columnsToIndex WHERE keyColumn = 1 AND isVariableLength = 1)
SET @Variable_Leaf_Size = 2 + (@Num_Variable_Leaf_Cols * 2) + (@Max_Var_Leaf_Size * @variableLengthStorageFullnessOfAllColumns)

DECLARE @Leaf_Row_Size int = @Fixed_Leaf_Size + @Variable_Leaf_Size + @Leaf_Null_Bitmap + 1
DECLARE @Leaf_Rows_Per_Page int = 8096 / (@Leaf_Row_Size + 2)
DECLARE @FillFactor int = 100
DECLARE @Free_Rows_Per_Page int = 0

--- 
SELECT @Num_Leaf_Cols AS Num_Leaf_Cols
	,@Fixed_Leaf_Size AS Fixed_Leaf_Size
	,@Num_Variable_Leaf_Cols AS Num_Variable_Leaf_Cols
	,@Max_Var_Leaf_Size AS Max_Var_Leaf_Size
	,@Leaf_Null_Bitmap AS Leaf_Null_Bitmap
	,@variableLengthStorageFullnessOfAllColumns AS StorageFullness
	,@Variable_Leaf_Size AS Variable_Leaf_Size
	,@Leaf_Row_Size AS Leaf_Row_Size
	,@Leaf_Rows_Per_Page AS Leaf_Rows_Per_Page
	,@FillFactor AS [FillFactor]
	,@Free_Rows_Per_Page AS Free_Rows_Per_Page

DECLARE @Num_Leaf_Pages bigint = @Num_Rows / (@Leaf_Rows_Per_Page - @Free_Rows_Per_Page)
DECLARE @Leaf_Space_Used bigint = 8192 * @Num_Leaf_Pages

SELECT @Num_Leaf_Pages AS Num_Leaf_Pages
	,@Leaf_Space_Used AS Leaf_Space_Used 
	,@Leaf_Space_Used / 1024 AS Leaf_Space_Used_kB
	,@Leaf_Space_Used / 1024 / 1024 AS Leaf_Space_Used_MB
	,@Leaf_Space_Used / 1024 / 1024 / 1024 AS Leaf_Space_Used_GB



/* Step 3. Calculate the Space Used to Store Index Information in the Non-leaf Levels */

/*I use '2 +' here instead of '1 +' as in the doc to cater for roundup */
DECLARE @Nonleaf_Levels int = 2 + log(@Num_Leaf_Pages / @Index_Rows_Per_Page, @Index_Rows_Per_Page)

DECLARE @lc int = @Nonleaf_Levels
DECLARE @Num_Index_Pages bigint = 0

WHILE @lc > 0
BEGIN
	SET @Num_Index_Pages += @Num_Leaf_Pages / POWER(@Index_Rows_Per_Page, @lc)
	SET @lc -= 1
END

DECLARE @Index_Space_Used bigint = 8192 * @Num_Index_Pages

---
SELECT @Nonleaf_Levels AS Nonleaf_Levels
	,@Num_Index_Pages AS Num_Index_Pages
	,@Index_Space_Used AS Index_Space_Used
	,@Index_Space_Used / 1024 AS Index_Space_Used_kB
	,@Index_Space_Used / 1024 / 1024 AS Index_Space_Used_MB
	,@Index_Space_Used / 1024 / 1024 / 1024 AS Index_Space_Used_GB

DECLARE @Estimated_Nonclustered_Index_Size bigint = @Leaf_Space_Used + @Index_Space_used

SELECT @Estimated_Nonclustered_Index_Size AS Estimated_Nonclustered_Index_Size
	,@Estimated_Nonclustered_Index_Size / 1024 AS Estimated_Nonclustered_Index_Size_kB
	,@Estimated_Nonclustered_Index_Size / 1024 / 1024 AS Estimated_Nonclustered_Index_Size_MB
	,@Estimated_Nonclustered_Index_Size / 1024 / 1024 / 1024 AS Estimated_Nonclustered_Index_Size_GB

IF @indexName <> '' BEGIN
	;WITH pages AS (
		SELECT SUM(used_pages) AS pages 
		FROM sys.indexes i 
		JOIN sys.partitions p ON p.object_id = i.object_id AND i.index_id = p.index_id
		JOIN sys.allocation_units au ON au.container_id = p.partition_id
		WHERE i.object_id=OBJECT_ID(@tableToIndex) AND i.[name] = @indexName
		)
	SELECT pages * 8192 AS ActualIndexSize
		,pages * 8192 / 1024 AS ActualIndexSize_kB
		,pages * 8192 / 1024 / 1024 AS ActualIndexSize_MB
		,pages * 8192 / 1024 / 1024 / 1024 AS ActualIndexSize_GB
	FROM pages
END
