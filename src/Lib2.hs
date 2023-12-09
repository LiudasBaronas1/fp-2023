{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE DeriveFunctor #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    Condition (..),
    loadFile
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row (..))
import InMemoryTables (TableName, database)
import Data.List (isPrefixOf, find, elemIndex, foldl')
import Data.Char (toLower, isSpace)
import Data.Maybe (fromJust, fromMaybe)
type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data Condition = OrCondition [Condition]
               | EqualCondition String Value
               | NotEqualCondition String Value
               | LessThanOrEqualCondition String Value
               | GreaterThanOrEqualCondition String Value
               | LessThanCondition String Value
               | GreaterThanCondition String Value
               deriving (Show, Eq)

data ParsedStatement
  = ShowTables
  | ShowTable TableName
  | Select [String] [TableName] (Maybe Condition) 
  | ParsedStatement
  | Max String TableName String
  | Avg String TableName String
  | Now
  | UnknownStatement
  | Update TableName [(String, Value)] (Maybe Condition)
  | Insert String [String] [[String]]
  | Delete TableName (Maybe Condition)
  deriving (Show, Eq)
data ExecutionAlgebra next 
  = LoadFile TableName (Either ErrorMessage DataFrame -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra
loadFile :: TableName -> Execution (Either ErrorMessage DataFrame)
loadFile name = liftF $ LoadFile name id

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement statement =
  case words (map toLower statement) of
    ["show", "tables"] -> Right ShowTables
    ["show", "table", tableName] -> Right (ShowTable tableName)
    ["select", "max(", columnName, ")", "from", tableName] -> Right (Max columnName tableName "Max Value")
    ["select", "avg(", columnName, ")", "from", tableName] -> Right (Avg columnName tableName "Average Value")
    ("select" : columns) ->
      case break (== "from") columns of
        (cols, "from" : rest) ->
          let (tableNames, conditions) = parseTableNamesAndConditions rest
          in Right (Select (splitColumns (unwords cols)) tableNames conditions)
        _ -> Left "Invalid SELECT statement"
    ["now()"] -> Right Now
    ("insert" : rest) -> parseInsert rest 
    _ -> Left "Not supported statement"

parseInsert :: [String] -> Either ErrorMessage ParsedStatement
parseInsert ("into" : tableName : rest) =
  case break (== "values") rest of
    (columnNames, "values" : values) ->
      let columns = splitColumns (unwords columnNames)
          rows = map (splitColumns . removePunctuation) values
      in
        if all (\row -> length columns == length row) rows
          then Right (Insert tableName columns rows)
          else Left "Number of columns and values mismatch in INSERT statement"
    _ -> Left "Invalid INSERT statement"
parseInsert _ = Left "Invalid INSERT statement"

removePunctuation :: String -> String
removePunctuation = filter (\c -> c /= '(' && c /= ')')

parseMultipleRows :: [String] -> [[String]]
parseMultipleRows [] = []
parseMultipleRows values =
  case break (== "),") values of
    (row, rest) -> splitColumns (unwords row) : parseMultipleRows (drop 1 rest)

parseTableNamesAndConditions :: [String] -> ([TableName], Maybe Condition)
parseTableNamesAndConditions [] = ([], Nothing)
parseTableNamesAndConditions ("where" : rest) =
  let (conditions, remaining) = parseConditions rest
  in ([], conditions)
parseTableNamesAndConditions ("or" : rest) =
  let (nextCondition, remaining) = parseConditions rest
  in case nextCondition of
       Just c -> ([], Just (OrCondition [c]))
       Nothing -> ([], Just (OrCondition []))
parseTableNamesAndConditions (tableName : rest) =
  let (tableNames, conditions) = parseTableNamesAndConditions rest
  in (splitTableNames tableName ++ tableNames, conditions)

splitColumns :: String -> [String]
splitColumns = words . map (\c -> if c == ',' then ' ' else c)

splitTableNames :: String -> [TableName]
splitTableNames = words . map (\c -> if c == ',' then ' ' else c)

parseConditions :: [String] -> (Maybe Condition, [String])
parseConditions [] = (Nothing, [])
parseConditions ("where" : rest) =
  let (conditions, remaining) = parseConditions rest
  in (conditions, remaining)
parseConditions ("or" : rest) =
  let (nextCondition, remaining) = parseConditions rest
  in case nextCondition of
       Just c -> (Just (OrCondition [c]), remaining)
       Nothing -> (Just (OrCondition []), remaining)
parseConditions (colName : op : value : rest) =
  let (nextCondition, remaining) = parseConditions rest
  in case op of
    "=" -> (Just $ combineCondition (EqualCondition colName (StringValue value)) nextCondition, remaining)
    "<>" -> (Just $ combineCondition (NotEqualCondition colName (StringValue value)) nextCondition, remaining)
    "<=" -> (Just $ combineCondition (LessThanOrEqualCondition colName (StringValue value)) nextCondition, remaining)
    ">=" -> (Just $ combineCondition (GreaterThanOrEqualCondition colName (StringValue value)) nextCondition, remaining)
    "<" -> (Just $ combineCondition (LessThanCondition colName (StringValue value)) nextCondition, remaining)
    ">" -> (Just $ combineCondition (GreaterThanCondition colName (StringValue value)) nextCondition, remaining)
    _ -> (Nothing, [])
  where
    combineCondition :: Condition -> Maybe Condition -> Condition
    combineCondition c (Just (OrCondition cs)) = OrCondition (c : cs)
    combineCondition c Nothing = c

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Unsupported statement"

mergeDataFrames :: [(TableName, DataFrame)] -> DataFrame
mergeDataFrames [] = error "No data frames to merge"
mergeDataFrames ((firstTableName, firstDF) : rest) =
  let mergeTwoDF :: TableName -> DataFrame -> DataFrame -> DataFrame
      mergeTwoDF currentTable nextDF mergedDF =
        let prefixedNextDF = addTablePrefix currentTable nextDF
            mergedRows = [row1 ++ row2 | row1 <- rows mergedDF, row2 <- rows prefixedNextDF]
        in DataFrame (columns mergedDF ++ columns prefixedNextDF) mergedRows

      addTablePrefix :: TableName -> DataFrame -> DataFrame
      addTablePrefix tableName' df' =
        let prefixColumns (Column name colType) = Column (tableName' ++ "." ++ name) colType
            prefixedCols = map prefixColumns (columns df')
        in DataFrame prefixedCols (rows df')
        
      prefixedFirstDF = addTablePrefix firstTableName firstDF
  in foldl' (\acc (t, d) -> mergeTwoDF t d acc) prefixedFirstDF rest

evalCondition :: Condition -> DataFrame -> Row -> Bool
evalCondition (OrCondition conditions) df row = any (\cond -> evalCondition cond df row) conditions
evalCondition (EqualCondition colName val) df row =
  if '.' `elem` valWithDot val
    then handleComparisonJoin (==) colName val df row
    else matchValue (getColumnValue colName df row) val
evalCondition (NotEqualCondition colName val) df row =
  if '.' `elem` valWithDot val
    then not (handleComparisonJoin (==) colName val df row)
    else not (matchValue (getColumnValue colName df row) val)
evalCondition (LessThanOrEqualCondition colName val) df row =
  if '.' `elem` valWithDot val
    then handleComparisonJoin (<=) colName val df row
    else compareValues (<=) colName val df row
evalCondition (GreaterThanOrEqualCondition colName val) df row =
  if '.' `elem` valWithDot val
    then handleComparisonJoin (>=) colName val df row
    else compareValues (>=) colName val df row
evalCondition (LessThanCondition colName val) df row =
  if '.' `elem` valWithDot val
    then handleComparisonJoin (<) colName val df row
    else compareValues (<) colName val df row
evalCondition (GreaterThanCondition colName val) df row =
  if '.' `elem` valWithDot val
    then handleComparisonJoin (>) colName val df row
    else compareValues (>) colName val df row
evalCondition _ _ _ = False

valWithDot :: Value -> String
valWithDot (StringValue s) = s
valWithDot _ = ""

handleComparisonJoin :: (String -> String -> Bool) -> String -> Value -> DataFrame -> Row -> Bool
handleComparisonJoin op colName val df row =
  case val of
    StringValue otherColName ->
      case (getColumnValue colName df row, getColumnValue otherColName df row) of
        (StringValue thisValue, StringValue otherValue) -> op thisValue otherValue
        _ -> False
    _ -> False

matchValue :: Value -> Value -> Bool
matchValue (StringValue s1) (StringValue s2) = map toLower s1 == map toLower s2
matchValue _ _ = False

trimValue :: Value -> Value
trimValue (StringValue s) = StringValue (trimWhitespace s)
trimValue v = v

getColumnValue :: String -> DataFrame -> Row -> Value
getColumnValue colName df row =
    let colIndex = columnIndex df (Column (trimWhitespace (map toLower colName)) StringType)
    in row !! colIndex

trimWhitespace :: String -> String
trimWhitespace = filter (not . isSpace)

compareValues :: (String -> String -> Bool) -> String -> Value -> DataFrame -> Row -> Bool
compareValues op colName val df row =
    case (trimValue (getColumnValue colName df row), trimValue val) of
        (StringValue s1, StringValue s2) -> op (map toLower s1) (map toLower s2)
        _ -> False

calculateAverage :: Int -> [Row] -> Value
calculateAverage colIndex rows =
  let total = sum [case row !! colIndex of { IntegerValue i -> i; _ -> 0 } | row <- rows]
      count = toInteger (length rows)
  in if count > 0 then IntegerValue (total `div` count) else NullValue

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

parseUpdates :: [String] -> ([(String, Value)], [String])
parseUpdates [] = ([], [])
parseUpdates (colName : "=" : value : rest) =
  let (updates, remaining) = parseUpdates rest
  in ((colName, parseValue value) : updates, remaining)
parseUpdates _ = ([], [])

-- Helper function to parse the values list in the INSERT statement
parseValues :: [String] -> [(String, Value)]
parseValues [] = []
parseValues (value : rest) =
  let values = parseValues rest
  in (value, parseValue value) : values

-- Helper function to parse a single value
parseValue :: String -> Value
parseValue s = StringValue s

-- Helper function to update a row with the given updates
updateRow :: [Column] -> [(String, Value)] -> Row -> Row
updateRow _ [] row = row
updateRow columnsToUpdate ((colName, newValue) : updates) row =
  let colIndex = fromMaybe (error "Column not found") (elemIndex (Column colName StringType) columnsToUpdate)
      updatedRow = take colIndex row ++ [newValue] ++ drop (colIndex + 1) row
  in updateRow columnsToUpdate updates updatedRow

