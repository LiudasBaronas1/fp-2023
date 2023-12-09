{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Replace case with fromMaybe" #-}
{-# HLINT ignore "Use zipWith" #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Eta reduce" #-}
{-# HLINT ignore "Use if" #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    dataFrameToJson
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..), Row(..))
import Data.Time ( UTCTime )
import Lib2 --(parseStatement, executeStatement, ParsedStatement(..), Condition(..), evalCondition, updateRow)
import Data.List (transpose, intercalate, isPrefixOf, find, elemIndex, foldl')
import Data.Aeson hiding (Value)
import Data.Aeson (encode, decode)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.ByteString.Lazy.Char8 as BS
import Control.Applicative ((<|>))
import System.Directory (createDirectoryIfMissing)
import System.FilePath ((</>), takeDirectory)
import InMemoryTables (database)
import Data.Char (toLower, isSpace)
import Data.Maybe (fromJust, fromMaybe)

type TableName = String
type FileContent = Either ErrorMessage DataFrame
type ErrorMessage = String

-- Change the type of SaveFile constructor to take a function (String, DataFrame) -> next
data ExecutionAlgebra next 
  = LoadFile TableName (Either ErrorMessage DataFrame -> next)
  | GetTime (UTCTime -> next)
  | SaveFile TableName DataFrame next
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution (Either ErrorMessage DataFrame)
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
  Left err -> return $ Left err
  Right statement -> do
    case statement of
      Now -> do
        currentTime <- getTime
        return $ Right $ DataFrame [Column "CurrentTime" StringType] [[StringValue (show currentTime)]]
      Insert n c v -> do
        currentTime <- getTime
        existingData <- loadFile n
        let columnNames = if null c then [n] else c
        let columns = map (\colName -> Column colName StringType) columnNames
        let values = map (\row -> map StringValue row) v
        let df = DataFrame columns values
        let mergedDf = mergeDataFrames existingData df
        saveFile n mergedDf
        return $ Right mergedDf
      Update n u c -> do
        existingData <- loadFile n
        let updates = map (\(colName, expr) -> (colName, evalExpr expr)) u
        let updatedDf = updateDataFrame existingData updates c
        saveFile n updatedDf
        return $ Right updatedDf
      Delete n condition -> do
        existingData <- loadFile n
        let updatedDf = deleteDataFrame existingData condition
        saveFile n updatedDf
        return $ Right updatedDf
      _ -> do
        let result = executeStatement statement
        return $ case result of
          Left err -> Left err
          Right df -> Right df
  where
  mergeDataFrames :: Either ErrorMessage DataFrame -> DataFrame -> DataFrame
  mergeDataFrames (Right (DataFrame existingColumns existingRows)) (DataFrame newColumns newRows) =
    DataFrame existingColumns (existingRows ++ newRows)
  mergeDataFrames _ newDf = newDf

  updateDataFrame :: Either ErrorMessage DataFrame -> [(String, Value)] -> Maybe Condition -> DataFrame
  updateDataFrame (Right (DataFrame existingColumns existingRows)) updates condition =
    let columnsToUpdate = map (\(colName, _) -> Column colName StringType) updates
        updatedRows = map (\row -> updateRow columnsToUpdate updates row) existingRows
        updatedData = DataFrame existingColumns updatedRows
    in case condition of
      Just cond -> filterDataFrame cond updatedData
      Nothing -> updatedData
  updateDataFrame _ _ _ = error "Invalid data frame for update"

  filterDataFrame :: Condition -> DataFrame -> DataFrame
  filterDataFrame condition (DataFrame columns rows) =
    let filteredRows = filter (\row -> evalCondition condition (DataFrame columns [row]) row) rows
    in DataFrame columns filteredRows

  deleteDataFrame :: Either ErrorMessage DataFrame -> Maybe Condition -> DataFrame
  deleteDataFrame (Right (DataFrame existingColumns existingRows)) condition =
    let updatedRows = case condition of
          Just cond -> filter (\row -> not (evalCondition cond (DataFrame existingColumns [row]) row)) existingRows
          Nothing -> []
    in DataFrame existingColumns updatedRows
  deleteDataFrame _ _ = error "Invalid data frame for delete"

evalExpr :: Value -> Value
evalExpr = id

-- insert works correctly only like this: insert employees id, name values (1,john), (2,mike)

saveFile :: TableName -> DataFrame -> Execution ()
saveFile tableName df = liftF $ SaveFile tableName df ()

columnTypeToJson :: ColumnType -> String
columnTypeToJson IntegerType = "integer"
columnTypeToJson StringType  = "string"
columnTypeToJson BoolType    = "bool"

columnToJson :: Column -> [(String, String)]
columnToJson (Column name colType) = [("name", name), ("type", columnTypeToJson colType)]

valueToJson :: Value -> String
valueToJson (IntegerValue x) = "{\"IntegerValue\":" ++ show x ++ "}"
valueToJson (StringValue x)  = "{\"StringValue\":\"" ++ x ++ "\"}"
valueToJson (BoolValue x)    = "{\"BoolValue\":" ++ if x then "true" else "false" ++ "}"
valueToJson NullValue        = "{\"NullValue\":null}"

rowToJson :: Row -> String
rowToJson = intercalate ", " . map valueToJson

dataFrameToJson :: DataFrame -> String
dataFrameToJson (DataFrame columns rows) =
  "{ \"columns\": [" ++ columnsJson ++ "], \"rows\": [" ++ rowsJson ++ "] }"
  where
    columnsJson = intercalate ", " $ map (\col -> "{ " ++ colJson col ++ " }") columns
    colJson (Column k v) = intercalate ", " $ map (\(k', v') -> "\"" ++ k' ++ "\": \"" ++ v' ++ "\"") [("name", k), ("type", columnTypeToJson v)]
    rowsJson = intercalate ", " $ map (\row -> "[" ++ rowToJson row ++ "]") rows

instance FromJSON ColumnType where
  parseJSON (String "integer") = return IntegerType
  parseJSON (String "string")  = return StringType
  parseJSON (String "bool")    = return BoolType
  parseJSON _                  = fail "Invalid ColumnType"

instance FromJSON Column where
  parseJSON (Object v) = Column <$> v .: "name" <*> v .: "type"
  parseJSON _ = fail "Invalid Column"

instance FromJSON Value where
  parseJSON (Object v) = IntegerValue <$> v .: "IntegerValue"
   <|> StringValue <$> v .: "StringValue"
   <|> BoolValue <$> v .: "BoolValue"
   <|> pure NullValue
  parseJSON _ = fail "Failed to parse Value"

instance FromJSON DataFrame where
  parseJSON (Object v) = DataFrame <$> v .: "columns" <*> v .: "rows"
  parseJSON _ = fail "Invalid DataFrame"