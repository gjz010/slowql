module Main where
    import qualified SlowQL.Test.TestRecord
    main :: IO()
    main = do
        putStrLn "SlowQL Test Started"
        SlowQL.Test.TestRecord.testTable