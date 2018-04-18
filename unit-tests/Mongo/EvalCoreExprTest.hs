module Mongo.EvalCoreExprTest(
    evalCoreExprTest
    ) where

import Mongo.CoreExpr
import Mongo.Error
import Mongo.EvalCoreExpr
import Mongo.Value
import Test.HUnit
import qualified Data.Either

emptyEnv :: Environment
emptyEnv = Environment { boundVariables = [], definedFunctions = [] }

emptyErr :: Error
emptyErr = Error { errCode = NotImplemented, errReason = "" }

asErr :: Either Error a -> Error
asErr (Left err) =
    err

asErr _ =
    emptyErr

evalCoreExprTest :: Test
evalCoreExprTest = TestList [
    "letBasic" ~: "" ~: Right (IntValue 3) ~=?
        evalCoreExpr (Let "x" (Const (IntValue 3)) (Var "x")) emptyEnv,

    "letWithShadowing" ~: "" ~: Right (IntValue 5) ~=?
        evalCoreExpr (Let "x" (Const (IntValue 3))
            (Let "x" (Const (IntValue 5)) (Var "x"))) emptyEnv,

    "unboundVariable" ~: "" ~: UnboundVariable ~=?
        errCode (asErr (evalCoreExpr (Var "y") Environment {
            boundVariables = [("x", IntValue 5)],
            definedFunctions = [] })),

    "functionDefinitionBasic" ~: "" ~: Right (IntValue 3) ~=?
        evalCoreExpr (FunctionDef "f" (Function ["x","y"] (PutBool (CompareEQ (Var "x") (Var "y"))))
            (Const (IntValue 3))) emptyEnv,

    "functionRedefinition" ~: "" ~: Right (IntValue 4) ~=?
            evalCoreExpr (FunctionDef "f"
                (Function ["x","y"] (PutBool (CompareEQ (Var "x") (Var "y"))))
                (FunctionDef "f" (Function [] (Const (IntValue 3)))
                    (Const (IntValue 4)))) emptyEnv,

    "functionApplicationBasic" ~: "" ~: Right (IntValue 8) ~=?
        evalCoreExpr (FunctionDef "f" (Function ["x","y"]
            (PutInt (Plus (GetInt (Var "x")) (GetInt (Var "y")))))
            (FunctionApp "f" [Const (IntValue 3), Const (IntValue 5)])) emptyEnv,

    "functionsDoNotSupportClosures" ~: "" ~: UnboundVariable ~=?
        errCode (asErr
            (evalCoreExpr (Let "x" (Const (IntValue 3))
                (FunctionDef "f" (Function [] (Var "x"))
                    (FunctionApp "f" []))) emptyEnv)),

    "functionDefinedFirstCanCallFunctionDefinedSecond" ~: "" ~: Right (IntValue 8) ~=?
        evalCoreExpr (FunctionDef "f" (Function ["x"] (FunctionApp "g" [Var "x"]))
            (FunctionDef "g" (Function ["y"] (Var "y"))
                (FunctionApp "f" [Const (IntValue 8)]))) emptyEnv,

    "andExprShortCircuits" ~: "" ~: Right False ~=?
        evalCoreExpr
            (And (GetBool (Const (BoolValue False))) (GetBool (Const (IntValue 3)))) emptyEnv,

    "orExprShortCircuits" ~: "" ~: Right True ~=?
        evalCoreExpr
            (Or (GetBool (Const (BoolValue True))) (GetBool (Const (IntValue 3)))) emptyEnv,

    "setFirstElem" ~: "" ~: Right Array { getElements = [IntValue 10, IntValue 1, IntValue 2] } ~=?
        evalCoreExpr
            (SetElem
                (0, Const (IntValue 10))
                (GetArray (Const (ArrayValue Array { getElements = [
                    IntValue 0,
                    IntValue 1,
                    IntValue 2] }))))
            emptyEnv,

    "setMidElem" ~: "" ~: Right Array { getElements = [IntValue 0, IntValue 11, IntValue 2] } ~=?
        evalCoreExpr
            (SetElem
                (1, Const (IntValue 11))
                (GetArray (Const (ArrayValue Array { getElements = [
                    IntValue 0,
                    IntValue 1,
                    IntValue 2] }))))
            emptyEnv,

    "setLastElem" ~: "" ~: Right Array { getElements = [IntValue 0, IntValue 1, IntValue 12] } ~=?
        evalCoreExpr
            (SetElem
                (2, Const (IntValue 12))
                (GetArray (Const (ArrayValue Array { getElements = [
                    IntValue 0,
                    IntValue 1,
                    IntValue 2] }))))
            emptyEnv,

    "appendElem" ~: "" ~:
        Right Array { getElements = [IntValue 0, IntValue 1, IntValue 2, IntValue 13] } ~=?
        evalCoreExpr
            (SetElem
                (3, Const (IntValue 13))
                (GetArray (Const (ArrayValue Array { getElements = [
                    IntValue 0,
                    IntValue 1,
                    IntValue 2] }))))
            emptyEnv,

    "nullFill" ~: "" ~:
        Right Array { getElements = [
            IntValue 0,
            IntValue 1,
            IntValue 2,
            NullValue,
            NullValue,
            IntValue 15] } ~=?
        evalCoreExpr
            (SetElem
                (5, Const (IntValue 15))
                (GetArray (Const (ArrayValue Array { getElements = [
                    IntValue 0,
                    IntValue 1,
                    IntValue 2] }))))
            emptyEnv
    ]
