{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..)
  )
where

import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row (..))
import InMemoryTables (TableName, database)
import Data.List(isPrefixOf, find, elemIndex, maximumBy)
import Data.Char(toLower)
import Data.Maybe (fromJust)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data ParsedStatement
  = ShowTables
  | ShowTable TableName
  | Select [String] TableName
  | ParsedStatement
  | Max String TableName String


-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement statement =
  case words (map toLower statement) of
    ["show", "tables"] -> Right ShowTables
    ["show", "table", tableName] -> Right (ShowTable tableName)
    ["max", columnName, "from", tableName] -> Right (Max columnName tableName "Max Value")
    ("select" : columns) ->
      case reverse columns of
        (tableName : revCols) -> Right (Select (reverse revCols) tableName)
        _ -> Left "Invalid SELECT statement"
    _ -> Left "Not supported statement"


-- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.database as a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTables = Right $ DataFrame [Column "Table Name" StringType] (map (\(name, _) -> [StringValue name]) database)
executeStatement (ShowTable tablename) =
  case lookup (map toLower tablename) database of
    Just df -> Right $ DataFrame [Column "Column Name" StringType] (map (\col -> [StringValue (columnName col)]) (columns df))
    Nothing -> Left "Table not found"
executeStatement (Select columnNames tableName) =
  case lookup (map toLower tableName) database of
    Just df -> do
      let selectedCols = filter (\col -> columnName col `elem` columnNames) (columns df)
      let selectedIndices = map (columnIndex df) selectedCols
      let selectedRows = map (\row -> map (\i -> row !! i) selectedIndices) (rows df)
      Right $ DataFrame selectedCols selectedRows
    Nothing -> Left "Table not found"
executeStatement (Max columnName tableName resultColumn) =
  case lookup (map toLower tableName) database of
    Just df -> do
      let colIndex = columnIndex df (Column columnName StringType)
      let nonNullRows = filter (\row -> case row !! colIndex of { NullValue -> False; _ -> True }) (rows df)
      let maxVal = case nonNullRows of
                     [] -> NullValue
                     _ -> foldl1 (maxValue colIndex (columnType (columns df !! colIndex))) (map (!! colIndex) nonNullRows)
      Right $ DataFrame [Column resultColumn (columnType (columns df !! colIndex))] [[maxVal]]
    Nothing -> Left "Table not found"

maxValue :: Int -> ColumnType -> Value -> Value -> Value
maxValue _ IntegerType (IntegerValue i1) (IntegerValue i2) = if i1 > i2 then IntegerValue i1 else IntegerValue i2
maxValue _ BoolType (BoolValue b1) (BoolValue b2) = if b1 > b2 then BoolValue b1 else BoolValue b2
maxValue _ StringType (StringValue s1) (StringValue s2) = if s1 > s2 then StringValue s1 else StringValue s2
maxValue _ _ val1 NullValue = val1
maxValue _ _ NullValue val2 = val2

maxValue colIndex colType _ _ = error $ "Column type mismatch for column at index " ++ show colIndex ++ ". Expected " ++ show colType

columnType :: Column -> ColumnType
columnType (Column _ colType) = colType

columnNames :: [Column] -> [String]
columnNames = map (\(Column name _) -> name)

columnIndex :: DataFrame -> Column -> Int
columnIndex (DataFrame cols _) col = case find (\c -> columnName c == columnName col) cols of
  Just foundCol -> fromJust $ elemIndex foundCol cols
  Nothing -> error "Column not found"

rows :: DataFrame -> [Row]
rows (DataFrame _ rs) = rs

columns :: DataFrame -> [Column]
columns (DataFrame cols _) = cols

columnName :: Column -> String
columnName (Column name _) = name

