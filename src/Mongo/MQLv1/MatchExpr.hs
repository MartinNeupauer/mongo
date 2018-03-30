module Mongo.MQLv1.MatchExpr(
    MatchExpr(..),

    evalMatchExpr,
    ) where

import Mongo.CoreExpr
import Mongo.Error
import Mongo.EvalCoreExpr
import Mongo.MQLv1.Path
import Mongo.Value

data MatchExpr
    = EqMatchExpr Path Value
    | LTEMatchExpr Path Value
    | LTMatchExpr Path Value
    | GTMatchExpr Path Value
    | GTEMatchExpr Path Value

-- Given a flag indicating array traversal behavior, the name of a function to apply, and the name
-- of a bound variable containing a value:
-- * If there is no implicit array traversal, returns a core expr which applies the named function
--   to the value bound to the var.
-- * If there is implicit array traversal, returns a core expr which folds over the array.
makeApplyFuncExpr :: ImplicitArrayTraversal -> String -> String -> CoreExpr Value
makeApplyFuncExpr NoImplicitTraversal funcName retrievedVal = FunctionApp funcName [Var retrievedVal]

makeApplyFuncExpr ImplicitlyTraverseArrays funcName retrievedVal =
    If (IsArray (Var retrievedVal))
        (let foldFuncName = "fold_" ++ funcName in
            FunctionDef foldFuncName (Function ["v", "init"]
                (PutBool (Or (GetBool (Var "init")) (GetBool (FunctionApp funcName [Var "v"])))))
                (PutBool (FoldBool
                    foldFuncName
                    (GetBool (Const (BoolValue False)))
                    (Var retrievedVal))))
        (FunctionApp funcName [Var retrievedVal])

-- Generates a core expression which traverses the given path, and applies the named function to
-- each leaf element. The expression returns true if the named function ever returns true.
makeTraversePathExpr :: Path -> String -> CoreExpr Value
makeTraversePathExpr [] funcName = FunctionApp funcName [Var "ROOT"]

makeTraversePathExpr (PathComponent (FieldName f) arrayTraversal : rest) funcName =
    let nextLevelFuncName = "get_" ++ f ++ "_level_" ++ show (length rest)
        formalParam = "curVal"
        retrievedVal = "value_of_" ++ f in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsDocument (Var formalParam)) (HasField f (GetDocument (Var formalParam))))
                (Let retrievedVal (SelectField f (GetDocument (Var formalParam)))
                    (makeApplyFuncExpr arrayTraversal funcName retrievedVal))
                (Const $ BoolValue False)))
            (makeTraversePathExpr rest nextLevelFuncName)

makeTraversePathExpr (PathComponent (ArrayIndex i) arrayTraversal : rest) funcName =
    let nextLevelFuncName = "get_" ++ show i ++ "_level_" ++ show (length rest)
        formalParam = "curVal"
        retrievedVal = "value_of_" ++ show i in
        FunctionDef nextLevelFuncName (Function [formalParam]
            (If (And (IsArray (Var formalParam)) (CompareLT (Const (IntValue i))
                        (PutInt (ArrayLength (GetArray (Var formalParam))))))
                (Let retrievedVal (SelectElem i (GetArray (Var formalParam)))
                    (makeApplyFuncExpr arrayTraversal funcName retrievedVal))
                (Const $ BoolValue False)))
            (makeTraversePathExpr rest nextLevelFuncName)

-- Compiles a MatchExpr into a core language expression.
desugarMatchExpr :: MatchExpr -> CoreExpr Bool
desugarMatchExpr (EqMatchExpr path val) =
    GetBool $ FunctionDef "eq" (Function ["x"] (PutBool $ CompareEQ (Var "x") (Const val)))
        (makeTraversePathExpr (reverse path) "eq")

-- Returns true if the Value matches the MatchExpr. Otherwise returns false. Any Error return value
-- is query-fatal.
evalMatchExpr :: MatchExpr -> Value -> Either Error Bool
evalMatchExpr matchExpr value =
    evalCoreExpr (desugarMatchExpr matchExpr)
        Environment { boundVariables = [("ROOT", value)], definedFunctions = [] }
