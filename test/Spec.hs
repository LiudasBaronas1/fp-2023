import Data.Either
import Data.Maybe ()
import DataFrame (Value(..), DataFrame(..), Column(..), ColumnType(StringType))
import InMemoryTables qualified as D
import Lib1
import Lib2
import Test.Hspec

main :: IO ()
main = hspec $ do
  
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
  
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()
  
  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null
  
  describe "Lib2.parseStatement" $ do
    it "parses SHOW TABLES statement" $ do
      Lib2.parseStatement "SHOW TABLES" `shouldBe` Right ShowTables
  
    it "parses SHOW TABLE statement" $ do
      Lib2.parseStatement "SHOW TABLE employees" `shouldBe` Right (ShowTable "employees")
  
    it "parses SELECT statement without WHERE clause" $ do
      Lib2.parseStatement "SELECT * FROM employees" `shouldBe` Right (Select ["*"] "employees" Nothing)
  
    it "parses SELECT statement with WHERE clause (string comparison)" $ do
      let statement = "SELECT name FROM employees WHERE name = Vi"
      Lib2.parseStatement statement `shouldBe` Right (Select ["name"] "employees" (Just (EqualCondition "name" (StringValue "vi"))))
  
    it "parses MAX statement" $ do
      Lib2.parseStatement "select MAX( name ) FROM employees" `shouldBe` Right (Max "name" "employees" "Max Value")
    
    it "parses AVG statement" $ do
      Lib2.parseStatement "select AVG( id ) FROM employees" `shouldBe` Right (Avg "id" "employees" "Average Value")
    
    it "handles invalid statements" $ do
      Lib2.parseStatement "INVALID STATEMENT" `shouldSatisfy` isLeft
    
    it "parses SELECT statement with WHERE OR clause (string comparison)" $ do
      let statement = "SELECT name FROM employees WHERE name = Vi or name = Ed"
      Lib2.parseStatement statement `shouldBe` Right (Select ["name"] "employees" (Just (OrCondition [EqualCondition "name" (StringValue "vi"), EqualCondition "name" (StringValue "ed")])))
  
  describe "Lib2.executeStatement" $ do
    it "executes SELECT statement with WHERE clause (string comparison)" $ do
      let statement = Select ["name"] "employees" (Just (EqualCondition "name" (StringValue "Vi")))
      Lib2.executeStatement statement `shouldBe`
        Right (DataFrame [Column "name" StringType] [[StringValue "Vi"]])
