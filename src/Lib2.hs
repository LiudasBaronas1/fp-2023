{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE DeriveFunctor #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    Condition (..),
    evalCondition,
    columnIndex,
    columnName,
    columnNames,
    updateRow,
    mergeSelectDataFrames,
    columns,
    rows,
    maxValue,
    columnType,
    calculateAverage
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (Column (..), ColumnType (..), Value (..), DataFrame (..), Row (..))
import InMemoryTables (TableName, database)
import Data.List (isPrefixOf, find, elemIndex, foldl', findIndex)
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
  | SelectWithNow [String] [String] (Maybe Condition)  -- Added a new constructor for SELECT with now()
  | UnknownStatement
  | Update TableName [(String, Value)] (Maybe Condition)
  | Insert String [String] [[String]]
  | Delete TableName (Maybe Condition)
  deriving (Show, Eq)

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement statement =
  if lastChar == ';'
    then case words (map toLower (init statement)) of
      ["show", "tables"] -> Right ShowTables
      ["show", "table", tableName] -> Right (ShowTable tableName)
      ["select", "max(", columnName, ")", "from", tableName] -> Right (Max columnName tableName "Max Value")
      ["select", "avg(", columnName, ")", "from", tableName] -> Right (Avg columnName tableName "Average Value")
{-      ("select" : columns) ->
        case break (== "from") columns of
          (cols, "from" : rest) ->
            let (tableNames, conditions) = parseTableNamesAndConditions rest
            in Right (Select (splitColumns (unwords cols)) tableNames conditions)
          _ -> Left "Invalid SELECT statement"-}
      ["select now()"] -> Right Now
      ("insert" : rest) -> parseInsert rest
      ("delete" : rest) -> parseDelete rest
      ("select" : rest) -> parseSelect rest
      ("update" : rest) -> parseUpdate rest
      _ -> Left "Not supported statement"
    else Left "Statement must end with a semicolon"
  where
    lastChar = if null statement then ' ' else last statement
    
parseSelect :: [String] -> Either ErrorMessage ParsedStatement
parseSelect columns =
  case break (== "from") columns of
    (cols, "from" : rest) ->
      let (tableNames, conditions) = parseTableNamesAndConditions rest
          selectCols = splitColumns (unwords cols)
      in if "now()" `elem` selectCols
           then Right (SelectWithNow selectCols tableNames conditions)
           else Right (Select selectCols tableNames conditions)
    _ -> Left "Invalid SELECT statement"
parseSelect _ = Left "Invalid SELECT statement"

parseDelete :: [String] -> Either ErrorMessage ParsedStatement
parseDelete ("from" : tableName : rest) =
  let (tableNames, conditions) = parseTableNamesAndConditions rest
  in Right (Delete tableName conditions)
parseDelete _ = Left "Invalid DELETE statement"

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

parseUpdate :: [String] -> Either ErrorMessage ParsedStatement
parseUpdate (tableName : "set" : rest) =
  case break (== "where") rest of
    (updates, "where" : conditions) -> do
      let updateList = parseUpdateList updates
      let (condition, _) = parseConditions conditions
      return $ Update tableName updateList condition
    (updates, []) -> do
      let updateList = parseUpdateList updates
      return $ Update tableName updateList Nothing
    _ -> Left "Invalid UPDATE statement"
parseUpdate _ = Left "Invalid UPDATE statement"

parseUpdateList :: [String] -> [(String, Value)]
parseUpdateList [] = []
parseUpdateList (colName : "=" : value : rest) =
  (colName, parseValue value) : parseUpdateList rest
parseUpdateList _ = []

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
executeStatement ShowTables = Right $ DataFrame [Column "Table Name" StringType] (map (\(name, _) -> [StringValue name]) database)
executeStatement (ShowTable tablename) =
  case lookup (map toLower tablename) database of
    Just df -> Right $ DataFrame [Column "Column Name" StringType] (map (\col -> [StringValue (columnName col)]) (columns df))
    Nothing -> Left "Table not found"
executeStatement (Select columnNames tableNames maybeCondition) = 
  case mapM (\tableName -> lookup (map toLower tableName) database) tableNames of
    Just dfs ->
      case dfs of
        [] -> Left "No tables provided"
        [singleDF] -> do
          let filteredRows = case maybeCondition of
                Just condition -> filter (\row -> evalCondition condition singleDF row) (rows singleDF)
                Nothing -> rows singleDF
          let selectedCols = if "*" `elem` columnNames
                             then columns singleDF
                             else filter (\col -> columnName col `elem` columnNames) (columns singleDF)
          let selectedIndices = map (columnIndex singleDF) selectedCols
          let selectedRows = map (\row -> map (\i -> row !! i) selectedIndices) filteredRows
          Right $ DataFrame selectedCols selectedRows
        multipleDFs -> do
          let mergedDF = mergeSelectDataFrames (zip tableNames dfs)
          let filteredRows = case maybeCondition of
                  Just condition -> filter (\row -> evalCondition condition mergedDF row) (rows mergedDF)
                  Nothing -> rows mergedDF
          let selectedCols = if "*" `elem` columnNames
                             then columns mergedDF
                             else filter (\col -> columnName col `elem` columnNames) (columns mergedDF)
          let selectedIndices = map (columnIndex mergedDF) selectedCols
          let selectedRows = map (\row -> map (\i -> row !! i) selectedIndices) filteredRows
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
executeStatement (Avg columnName tableName resultColumn) =
  case lookup (map toLower tableName) database of
    Just df -> do
      let colIndex = columnIndex df (Column columnName StringType)
      let nonNullRows = filter (\row -> case row !! colIndex of { NullValue -> False; _ -> True }) (rows df)
      let avgVal = case nonNullRows of
                     [] -> NullValue
                     _ -> calculateAverage colIndex nonNullRows
      Right $ DataFrame [Column resultColumn (columnType (columns df !! colIndex))] [[avgVal]]
    Nothing -> Left "Table not found"
executeStatement _ = Left "Unsupported statement"

mergeSelectDataFrames :: [(TableName, DataFrame)] -> DataFrame
mergeSelectDataFrames [] = error "No data frames to merge"
mergeSelectDataFrames ((firstTableName, firstDF) : rest) =
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
  let nonNullRows = filter (\row -> case row !! colIndex of { NullValue -> False; _ -> True }) rows
      total = sum [case row !! colIndex of { IntegerValue i -> i; _ -> 0 } | row <- nonNullRows]
      count = toInteger (length nonNullRows)
  in if count > 0 then IntegerValue (fromIntegral total `div` fromIntegral count) else NullValue

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
updateRow columnsToUpdate [] row = row
updateRow columnsToUpdate ((colName, newValue) : updates) row =
  case findIndex (\col -> columnName col == colName) columnsToUpdate of
    Just colIndex ->
      let updatedRow = take colIndex row ++ [newValue] ++ drop (colIndex + 1) row
      in updateRow columnsToUpdate updates updatedRow
    Nothing ->
      updateRow columnsToUpdate updates row
