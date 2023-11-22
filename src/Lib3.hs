{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..)
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..), Row(..))
import Data.Time ( UTCTime )

import Lib2 (parseStatement, executeStatement, ParsedStatement(..))

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

updateTable :: TableName -> DataFrame -> Execution DataFrame
updateTable name df = liftF $ UpdateTable name df id

insertIntoTable :: TableName -> DataFrame -> Execution DataFrame
insertIntoTable name df = liftF $ InsertIntoTable name df id

deleteFromTable :: TableName -> Execution DataFrame
deleteFromTable name = liftF $ DeleteFromTable name id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
  Left err -> return $ Left err
  Right statement -> do
    case statement of
      Now -> do
        currentTime <- getTime
        return $ Right $ DataFrame [Column "CurrentTime" StringType] [[StringValue (show currentTime)]]
      _ -> do
        let result = executeStatement statement
        return $ case result of
          Left err -> Left err
          Right df -> Right df
