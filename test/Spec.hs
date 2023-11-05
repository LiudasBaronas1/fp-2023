import Data.Either
import Data.Maybe ()
import DataFrame (Value(..), DataFrame(..), Column(..), ColumnType(StringType), ColumnType(IntegerType))
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
    
    it "parses SELECT statement with WHERE clause when its not equal (string comparison)" $ do
      let statement = "SELECT name FROM employees WHERE name <> Vi"
      Lib2.parseStatement statement `shouldBe` Right (Select ["name"] "employees" (Just (NotEqualCondition "name" (StringValue "vi"))))  
    
    it "parses SELECT statement with WHERE clause more or equal (equal) (string comparison)" $ do
      let statement = "SELECT name FROM employees WHERE name >= Vi"
      Lib2.parseStatement statement `shouldBe` Right (Select ["name"] "employees" (Just (GreaterThanOrEqualCondition "name" (StringValue "vi"))))
    
    it "parses SELECT statement with WHERE clause more or equal (greater) (string comparison)" $ do
      let statement = "SELECT name FROM employees WHERE name >= ed"
      Lib2.parseStatement statement `shouldBe` Right (Select ["name"] "employees" (Just (GreaterThanOrEqualCondition "name" (StringValue "ed"))))

    it "parses MAX statement" $ do
      Lib2.parseStatement "select MAX( name ) FROM employees" `shouldBe` Right (Max "name" "employees" "Max Value")
    
    it "parses AVG statement" $ do
      Lib2.parseStatement "select AVG( id ) FROM employees" `shouldBe` Right (Avg "id" "employees" "Average Value")
    
    it "handles invalid statements" $ do
      Lib2.parseStatement "INVALID STATEMENT" `shouldSatisfy` isLeft
    
    it "parses SELECT statement with WHERE OR clause (string comparison)" $ do
      let statement = "SELECT name FROM employees WHERE name = Vi or name = Ed"
      Lib2.parseStatement statement `shouldBe` Right (Select ["name"] "employees" (Just (OrCondition [EqualCondition "name" (StringValue "vi"), EqualCondition "name" (StringValue "ed")])))
  
    it "parses SELECT statement with multiple WHERE OR clauses (string comparison)" $ do
      let statement = "SELECT name FROM employees WHERE name = fakename or name = Ed or surname = Po"
      Lib2.parseStatement statement `shouldBe` Right (Select ["name"] "employees" (Just (OrCondition [EqualCondition "name" (StringValue "fakename"), OrCondition [EqualCondition "name" (StringValue "ed"), EqualCondition "surname" (StringValue "po")]])))
  
    it "parses SELECT statement with WHERE OR clause when value is larger (string comparison)" $ do
      let statement = "SELECT name FROM employees WHERE id > 0"
      Lib2.parseStatement statement `shouldBe` Right (Select ["name"] "employees" (Just (GreaterThanCondition "id" (StringValue "0"))))
  
    it "parses SELECT statement with WHERE OR clause when value is lesser (string comparison)" $ do
      let statement = "SELECT name FROM employees WHERE id < 0"
      Lib2.parseStatement statement `shouldBe` Right (Select ["name"] "employees" (Just (LessThanCondition "id" (StringValue "0"))))
  
  describe "Lib2.executeStatement" $ do
    it "executes SELECT statement with WHERE clause (string comparison)" $ do
      let statement = Select ["name"] "employees" (Just (EqualCondition "name" (StringValue "Vi")))
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "name" StringType] [[StringValue "Vi"]])
      
    it "executes SELECT statement without WHERE clause" $ do
      let statement = Select ["*"] "employees" Nothing
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
      
    it "executes MAX statement" $ do
      let statement = Max "id" "employees" "Max Value"
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "Max Value" IntegerType] [[IntegerValue 2]])
    
    it "executes AVG statement" $ do
      let statement = Avg "id" "employees" "Average Value"
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "Average Value" IntegerType] [[IntegerValue 1]])

    it "executes SELECT statement with string NOT EQUAL condition" $ do
      let statement = Select ["name"] "employees" (Just (NotEqualCondition "name" (StringValue "Ed")))
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "name" StringType] [[StringValue "Vi"]])

    it "executes SELECT statement with string EQUAL condition" $ do
      let statement = Select ["name"] "employees" (Just (EqualCondition "name" (StringValue "vi")))
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "name" StringType] [[StringValue "Vi"]])
    it "executes SELECT statement with string GREATER THAN condition" $ do
      let statement = Select ["name"] "employees" (Just (GreaterThanCondition "name" (StringValue "Ed")))
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "name" StringType] [[StringValue "Vi"]])

    it "executes SELECT statement with string LESS THAN condition" $ do
      let statement = Select ["name"] "employees" (Just (LessThanCondition "name" (StringValue "Vi")))
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "name" StringType] [[StringValue "Ed"]])

    it "executes SELECT statement with string GREATER THAN OR EQUAL condition" $ do
      let statement = Select ["name"] "employees" (Just (GreaterThanOrEqualCondition "name" (StringValue "Vi")))
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "name" StringType] [[StringValue "Vi"]])

    it "executes SELECT statement with string LESS THAN OR EQUAL condition" $ do
      let statement = Select ["name"] "employees" (Just (LessThanOrEqualCondition "name" (StringValue "Vi")))
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "name" StringType] [[StringValue "Vi"], [StringValue "Ed"]])
      
    it "executes SELECT statement with OR condition" $ do
      let statement = Select ["name"] "employees" (Just (OrCondition [EqualCondition "name" (StringValue "Vi"), EqualCondition "name" (StringValue "Ed")]))
      Lib2.executeStatement statement `shouldBe` Right (DataFrame [Column "name" StringType] [[StringValue "Vi"], [StringValue "Ed"]])