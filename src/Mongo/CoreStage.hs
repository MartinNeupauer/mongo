module Mongo.CoreStage (
    CoreStage(..),
    NamespaceString(..),

    evalCoreStage,
    ) where

import Control.Monad (filterM)
import Mongo.CoreExpr
import Mongo.Database
import Mongo.Error
import Mongo.EvalCoreExpr
import Mongo.Value

-- Internal representation for a query execution plan involving one or more collections.
data CoreStage
   = CoreStageFilter (CoreExpr Bool) CoreStage
   | CoreStageScan NamespaceString

-- Returns the collection inside a database instance by its name. Returns an empty collection if no
-- such namespace exists.
lookupCollection :: NamespaceString -> DatabaseInstance -> Collection
lookupCollection nsString (DatabaseInstance db) = case lookup nsString db of
    Just coll -> coll
    _ -> Collection []

-- Apply a core expression that returns a boolean to a value, for CoreStageFilter.
evalFilter :: CoreExpr Bool -> Value -> Either Error Bool
evalFilter expr val =
    evalCoreExpr expr Environment { boundVariables = [("ROOT", val)], definedFunctions = [] }

-- Executes a query plan against a particular database instance. Returns either an error, or the
-- result set of the query represented as a collection.
evalCoreStage :: CoreStage -> DatabaseInstance -> Either Error Collection
evalCoreStage (CoreStageScan nsString) db = Right $ lookupCollection nsString db

evalCoreStage (CoreStageFilter expr child) db =
    case evalCoreStage child db of
        Right (Collection results) -> filterM (evalFilter expr) results >>= Right . Collection
        Left e -> Left e
