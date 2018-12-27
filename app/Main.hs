module Main where
    import Control.Monad.State
    import qualified SlowQL.PageFS
    import qualified Data.ByteArray as BA
    import qualified SlowQL.Manage.Database as DB
    import qualified SlowQL.SQL.Parser as P
    import Data.IORef
    import Data.Maybe
    import Control.Exception
    import Data.Either
    import System.IO
    import System.Directory
    import Control.Monad
    import qualified Data.ByteString.Char8 as BC
    data REPLState=REPLState {database :: Maybe DB.Database}
    withDatabase :: REPLState->(DB.Database->IO ())->IO REPLState
    withDatabase state f=
        if (isNothing $ database state)
            then do
                    putStrLn "This statement requires a database!"
                    return state
            else do
                    let Just db=database state
                    f db
                    return state
    printREPL :: REPLState->IO()
    printREPL state=do
        putStr "SlowQL ("
        dbname<-if isNothing $ database state
                    then return "[null]"
                    else do
                        let Just db=database state
                        df<-readIORef $ DB.def db
                        return $ BC.unpack $ DB.name df
        putStr dbname
        putStr ")> "
        hFlush stdout
    readSQL :: String->IO (Maybe P.SQLStatement)
    readSQL sql=do
        tree<-try $ evaluate (P.parseSQL sql) :: IO (Either SomeException P.SQLStatement)
        if(isLeft tree) then do
            let Left ex=tree
            putStrLn "Error while parsing SQL statement!"
            print ex
            return Nothing
        else do
            let Right t=tree
            return $ Just t
    repl :: REPLState->IO()
    repl state=do
        printREPL state
        input<-getLine
        case input of
            "\\q" -> do
                        if isJust $ database state
                            then do
                                    let Just db=database state
                                    DB.close db
                            else return ()
                        putStrLn "Bye-Bye!"
            "" -> repl state
            otherwise -> do
                            sql<-readSQL input
                            if isJust sql then do
                                enstate<-try $ execute state (fromJust sql) :: IO (Either SomeException REPLState)
                                if isLeft enstate
                                    then let Left ex =enstate in print ex >>= (const $ repl state)
                                    else let Right nstate=enstate in repl nstate
                            else repl state
    
    execute :: REPLState->P.SQLStatement->IO REPLState
    execute state P.ShowDatabases=(putStr "Databases: ") >>= (const DB.listDatabases) >>= print >>= (const $ return state)
    execute state (P.CreateDatabase db)=do
        r<-DB.create db
        if isJust r
            then do
                let Just err=r in putStrLn $ "Error: "++err
            else do
                putStrLn "Database Created."
        return state
    execute state (P.DropDatabase db)=do
        r<-DB.drop db
        if isJust r
            then do
                let Just err=r in putStrLn $ "Error: "++err
                return state
            else do
                putStrLn "Database Dropped."
                return state

    execute state (P.UseDatabase db)=do
        let reopen_check=if(isJust $ database state)
                then do
                    let Just dbo=database state
                    d<-readIORef $ DB.def dbo
                    let n=DB.name d
                    return $ (BC.pack db)==n
                else return False
        ret<-reopen_check
        if ret
            then do
                putStrLn "This is already current database."
                return state;
            else do
                d<-DB.open db
                if isRight d
                    then do
                        let Right cd=d
                        when (isJust $ database state) $ do
                            let Just jd=database state
                            DB.close jd
                        return $ REPLState (Just cd)
                    else do
                        let Left err=d
                        putStrLn $ "Error while using database: "++err
                        return state
    --Table related
    execute state (P.CreateTable name (P.TableDescription params primary fkey))=
        withDatabase state $ \db->DB.createTable db name params primary fkey

    execute state (P.ShowTables)=
        withDatabase state $ DB.showTables
    execute state (P.SelectStmt columns tables whereclause)=
        withDatabase state $ DB.selectFrom (P.SelectStmt columns tables whereclause)
    execute state (P.SelectAllStmt tables whereclause)=
        withDatabase state $ DB.selectFrom (P.SelectAllStmt tables whereclause)


    execute state (P.InsertStmt a b)=
        withDatabase state $ DB.doInsert a b
    execute state sql=(putStrLn ("Not implemented yet! "++(show sql))) >>= (const $ return state)
    
    initialize :: IO()
    initialize = do
        e<-doesDirectoryExist "slowql-data"
        if (not e) then
            do
                putStrLn "slowql-data not found. Creating the directory..."
                createDirectory "slowql-data"
                
        else return ()
        putStrLn "Initialization Done."
    main :: IO()
    main = do 
        putStrLn "\nSlowQL Functional RDBMS"
        initialize
        putStrLn "Please select your database before your operations... \n"
        repl (REPLState Nothing)

        -- table <- SlowQL.PageFS.openTableFile "table.tbl"
        --test_pagefs
        return ()

{-
    test_pagefs :: IO()
    test_pagefs = do
        table <- SlowQL.PageFS.openDataFile "table.tbl"
        let arr1=BA.pack [50|a<-[1..8192]]
        SlowQL.PageFS.writePage table 0 arr1
        let arr2=BA.pack [52|a<-[1..8192]]
        size<-SlowQL.PageFS.getFileSize table
        print size
        SlowQL.PageFS.writePage table 11 arr2
        size<-SlowQL.PageFS.getFileSize table
        print size
        SlowQL.PageFS.writeBackAll table
        size<-SlowQL.PageFS.getFileSize table
        print size
        page<-SlowQL.PageFS.readPageLazy table 100
        size<-SlowQL.PageFS.getFileSize table
        print size
        print $ BA.length page
        SlowQL.PageFS.closeDataFile table
        putStrLn "Done"
-}