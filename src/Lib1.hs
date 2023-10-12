{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..), Row)
import InMemoryTables (TableName)
import Data.Char (toLower)

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName db tableName =
  lookup (map toLower tableName) db

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement "" = Left "Empty SQL Statement"
parseSelectAllStatement statement
  | last cleanedStatement == ';' && length cleanedStatement > 1 =
    validTableName (init cleanedStatement)
  | otherwise = validTableName cleanedStatement
  where
    cleanedStatement = map toLower statement
    validTableName stmt =
      case words stmt of
        ["select", "*", "from", tableName] -> Right tableName
        _ -> Left "Invalid SQL Statement"

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame cols rows) = do
  mapM_ (validateRow cols) rows

validateRow :: [Column] -> [Value] -> Either ErrorMessage ()
validateRow cols values
  | length cols /= length values = Left "Number of columns does not match number of values."
  | otherwise = mapM_ (uncurry validateValue) (zip cols values)

validateValue :: Column -> Value -> Either ErrorMessage ()
validateValue (Column _ colType) value = 
  case (colType, value) of
    (_, NullValue) -> Right ()
    (IntegerType, IntegerValue _) -> Right ()
    (StringType, StringValue _) -> Right ()
    (BoolType, BoolValue _) -> Right ()
    (_, _) -> Left "Value type does not match column type."

getColType :: Column -> ColumnType
getColType (Column _ t) = t

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable maxWidth (DataFrame columns rows) =
    let colWidths = calculateColumnWidths maxWidth columns
        renderedRows = renderRows rows colWidths
        table = renderMainTableHeader columns colWidths
        separator = renderSeparatorLine maxWidth columns
    in table ++ "\n" ++ separator ++ "\n" ++ renderedRows ++ "\n"

calculateColumnWidths :: Integer -> [Column] -> [Int]
calculateColumnWidths maxWidth colNames =
    let numCols = length colNames 
        availableWidth = maxWidth - 3 
        cellWidth = availableWidth `div` fromIntegral numCols 
    in replicate numCols (fromIntegral cellWidth) 

renderSeparatorLine :: Integer -> [Column] -> String
renderSeparatorLine maxWidth colNames =
    let numCols = length colNames 
        availableWidth = maxWidth - 3 
        cellWidth = availableWidth `div` fromIntegral numCols 
        separatorWidth = numCols * fromIntegral cellWidth + 3 
    in replicate separatorWidth '-' 

renderCell :: Value -> Int -> String
renderCell value width = case value of
    (StringValue s) -> renderStringCell s width
    (IntegerValue i) -> renderIntegerCell i width
    (BoolValue b) -> renderBoolCell b width
    NullValue -> renderNullCell width
    

renderStringCell :: String -> Int -> String
renderStringCell s width
    | length s <= availableWidth = s ++ replicate paddingSpaces ' ' ++ "|"
    | otherwise = take (availableWidth - 3) s ++ "..." ++ "|"
    where
        availableWidth = width - 1
        paddingSpaces = availableWidth - length s

renderIntegerCell :: Integer -> Int -> String
renderIntegerCell i width = show i ++ replicate paddingSpaces ' ' ++ "|"
    where
        availableWidth = width - 1 
        paddingSpaces = availableWidth - length (show i)
    
renderBoolCell :: Bool -> Int -> String
renderBoolCell b width = (if b then "True" else "False") ++ replicate paddingSpaces ' ' ++ "|"
    where
        availableWidth = width - 1 
        paddingSpaces = availableWidth - length (if b then "True" else "False")
    
renderNullCell :: Int -> String
renderNullCell width = replicate (width - 1) ' ' ++ "|"

renderMainTableHeader :: [Column] -> [Int] -> String
renderMainTableHeader colNames columnWidths =
    let headerCells = zipWith renderHeaderCell colNames columnWidths
    in "|" ++ concat headerCells

renderHeaderCell :: Column -> Int -> String
renderHeaderCell (Column name _) width =
    renderCell (StringValue name) width

renderRows :: [Row] -> [Int] -> String
renderRows dataRows columnWidths =
    let renderedDataRows = map (renderSingleRow columnWidths) dataRows
    in unlines renderedDataRows

renderSingleRow :: [Int] -> Row -> String
renderSingleRow widths row =
    "|" ++ concat [renderCell val width | (val, width) <- zip row widths] 
