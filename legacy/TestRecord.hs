module SlowQL.Test.TestRecord where
    import qualified SlowQL.Table as T
    import qualified SlowQL.TableIndex as I
    import SlowQL.DataType
    import Data.Maybe
    import Data.Either
    import System.IO
    import System.Directory
    import Control.Exception
    import System.IO.Error hiding (catch)

    removeIfExists :: FilePath -> IO ()
    removeIfExists fileName = removeFile fileName `catch` handleExists
        where handleExists e
                | isDoesNotExistError e = return ()
                | otherwise = throwIO e
    
    testTable :: IO()

    e2m :: Either b a->(Maybe b, Maybe a)
    e2m (Right a)=(Nothing, Just a)
    e2m (Left b)=(Just b, Nothing)
    tryClose :: Maybe T.Table->IO()
    tryClose Nothing=return ()
    tryClose (Just t)=T.closeTable t
    printIO :: T.Record->IO()->IO ()
    printIO x a=do
        print x
        a
    bulkInsert :: T.Table->[T.Record]->IO (Either [DataWriteError] T.Table)
    bulkInsert table []=return $ Right table
    bulkInsert table (x:xs)=do
        either<-T.insert table x
        if(isLeft either) then 
            let (Left error)=either in return (Left error)
        else
            let (Right t2)=either in bulkInsert t2 xs
    
    testTable = do
        putStrLn "Testing Record Manager"
        putStrLn "Doing cleanup job"
        removeIfExists "slowql_test.tbl.dat"
        createTable 
        (Just table)<-T.openTable "slowql_test"
        (e1, t1)<-fmap e2m $ T.insert table [ValChar $ Just "gjz010", ValInt $ Just 60]
        --print (e1, t1)
        let (Just t1')=t1
        --(e2, t2)<-fmap e2m $ bulkInsert t1' [[ValChar $ Just ("user"++(show i)) , ValInt $ Just i]|i<-[1..10000]]
        --let (Just t2')=t2
        let t2'=t1'
        print t2'
        T.closeTable t2'
        putStrLn "Now let's try to open this table again"
        
        (Just table')<-T.openTable "slowql_test"
        print table'
        everything<- T.foldrQuery (:) [] table'
        print $ length everything
        T.closeTable table'
        --print everything 
        createIndex

        return ()
    
    createTable = do
        putStrLn "Creating test table"
        T.createTable "slowql_test" [TVarCharParam (defaultGeneral "name") 16, TIntParam (defaultGeneral "score") 0]
        table<-T.openTable "slowql_test"
        if isJust table  then do
            let (Just t)=table
            print t
            T.closeTable t
            return ()
        else do
            putStrLn "Create Failed!"
            return ()
            

    --bulkInsertIndex index values=
    createRawData id=[ValBool (Just True), ValInt (Just id), ValChar $ Just ("user_"++(show id)), ValInt $ Just id]
    insertIndexRaw index l=case l of
        
        []-> return index
        (x:xs)-> do
            -- putStrLn $ show x
            new_index<-I.insert index $ createRawData x
            insertIndexRaw new_index xs
    createIndex = do
        putStrLn "Creating Index"
        Just table<-T.openTable "slowql_test"

        index<-I.createIndex table "index_test" [("score", False)]
        putStrLn $ show index
        new_index<-insertIndexRaw index ([1..10000])
        putStrLn $ show new_index

        I.closeIndex index
        