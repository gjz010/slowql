
module Main where
    import qualified SlowQL.Test.LinearTableTest
    import qualified SlowQL.Test.BTreeTableTest
    main :: IO()
    main = do
        putStrLn "SlowQL Test Started"
        SlowQL.Test.LinearTableTest.test
        SlowQL.Test.BTreeTableTest.test
