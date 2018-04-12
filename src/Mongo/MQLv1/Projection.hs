module Mongo.MQLv1.Projection(
    InclusionProjectionNode(..),

    evalInclusionProjection,
    ) where

import Mongo.CoreExpr
import Mongo.Error
import Mongo.EvalCoreExpr
import Mongo.MQLv1.Path
import Mongo.Value
import Control.Monad (foldM)

newtype InclusionProjectionNode = InclusionProjectionNode [(PathComponent, InclusionProjectionNode)]

-- Generates a core language expression which sets the value of field 'f' in the result of running
-- expression 'expr' to the Value bound to the variable 'varName'.
addProjectedField :: String -> String -> CoreExpr Value -> CoreExpr Value
addProjectedField f varName expr = PutDocument $ SetField (f, Var varName) (GetDocument expr)

-- Given an expression that applies a projection subtree to the variable "ROOT", returns an
-- expression which rebinds the value of the variable "value" to "ROOT", applies the projection, and
-- adds the results to the output document that we're accumulating.
--
-- 'resultExpr' describes the output Document in which we're accumulating the results of the
-- projection. 'f' is the field name with which we should add the result of the projection to
-- 'resultExpr'.
addSubProjection :: String -> CoreExpr Value -> CoreExpr Value -> CoreExpr Value
addSubProjection f projectRootExpr resultExpr =
    -- We rebind ROOT to the subdocument that is being projected.
    Let "ROOT" (Var "value")
        (Let "newVal" projectRootExpr (addProjectedField f "newVal" resultExpr))

-- Projection of each level of a Document is implemented in terms of the core language by folding
-- over a Document with FoldValue. For instance, suppose at a particular level of the projection, we
-- wish to include path components "a" and "b". In this case, we will generate a FoldValue whose
-- accumulator function looks something like this (as a pseudocode representation of the core
-- expression language). The caller of accumInclusion ensures that the variables "fieldName" and
-- "value" are in scope.
--
--  function foldFunc("fieldName", "value", "accum") =
--    if (fieldName == "a")
--      (addField "a" (recursively project value) accum)
--    else if (fieldName == "b")
--      (addField "b" (recursively project value) accum)
--    else
--      accum
--
-- This function generates the function above when passed to a fold at compile time. Note that there
-- are two folds going on here: one to generate the chained If expression above as part of the
-- desugaring operation, and another FoldValue expression which will fold over the Document being
-- projected at runtime.
accumInclusion :: CoreExpr Value -> (PathComponent, InclusionProjectionNode)
    -> Either Error (CoreExpr Value)

accumInclusion expr (PathComponent (FieldNameOrArrayIndex i) _, _) = Left Error {
    errCode = IllegalProjection,
    errReason = "Field name or array index path component not allowed in inclusion projection: "
        ++ show i }

-- TODO WRITING-2743: Implement inclusion projection trees with array index path components. This is
-- not supported by the server, but it would make sense for the model to define the semantics.
accumInclusion expr (PathComponent (ArrayIndex i) _, _) = Left Error {
    errCode = NotImplemented,
    errReason = "Array index path component in inclusion projection is not yet implemented: "
        ++ show i }

-- We've reached a leaf of the projection tree.
accumInclusion expr (PathComponent (FieldName f) _, InclusionProjectionNode []) =
    return $ If (CompareEQ (Var "fieldName") (Const (StringValue f)))
        (addProjectedField f "value" expr)
        expr

accumInclusion expr (PathComponent (FieldName f) NoImplicitTraversal, projSubtree) = do
    desugarResult <- desugarInclusionProjection projSubtree
    let desugaredProjSubtree = PutDocument desugarResult in
        return $ If (And (CompareEQ (Var "fieldName") (Const (StringValue f)))
            -- XXX: Inclusion paths which traverse through scalars exclude rather than include those
            -- scalars. For instance, projecting {"a": 1} by {"a.b": 1} should result in the empty
            -- document.
            (IsDocument (Var "value")))
            (addSubProjection f desugaredProjSubtree expr)
            expr

accumInclusion expr (PathComponent (FieldName f) ImplicitlyTraverseArrays, projSubtree) = do
    desugarResult <- desugarInclusionProjection projSubtree
    let desugaredProjSubtree = PutDocument desugarResult in
        return $ If (Not (CompareEQ (Var "fieldName") (Const (StringValue f))))
            expr
            (If (IsDocument (Var "value"))
                (addSubProjection f desugaredProjSubtree expr)
                -- If the Value being included is an is an array, this is not a leaf of the
                -- projection tree, and this path component uses implicit array traversal, then
                -- traverse the array and apply the remaining projection to all subdocuments. This
                -- is done by folding over the array in order to produce a new array that contains
                -- the results of the projection.
                (If (IsArray (Var "value"))
                    (FunctionDef "foldFunc" (Function ["foldVal", "init"]
                        -- XXX: The projection excludes any array elements that aren't subdocuments.
                        -- Is this desirable behavior?
                        (If (Not (IsDocument (Var "foldVal")))
                            (Var "init")
                            (Let "ROOT" (Var "foldVal")
                                (Let "newVal" desugaredProjSubtree
                                    (PutArray (SetElem
                                        (ArrayLength (GetArray (Var "init")), Var "newVal")
                                        (GetArray (Var "init"))))))))
                        -- TODO WRITING-2728: We're only folding over one level of arrays. However,
                        -- in MQL, projection traverses nested arrays recursively. This cannot yet
                        -- be expressed in the core language.
                        (Let "newVal" (FoldValue "foldFunc"
                            (Const $ ArrayValue Array { getElements = []})
                            (Var "value"))
                            (addProjectedField f "newVal" expr)))
                    -- XXX: Inclusion paths which traverse through scalars exclude rather than
                    -- include those scalars. For instance, projecting {"a": 1} by {"a.b": 1}
                    -- results in the empty document in MQLv1.
                    expr))

-- Returns a one-argument function which, when applied to a Document, computes the given projection
-- and returns the resulting Document.
makeProjectSubtreeFunc :: InclusionProjectionNode -> Either Error Function
makeProjectSubtreeFunc (InclusionProjectionNode inclusions) = do
    foldFuncBody <- foldM accumInclusion (Var "init") inclusions
    return $ Function ["x"] (FunctionDef "foldFunc" (Function ["foldVal", "init"]
        (Let "fieldName" (SelectElem 0 (GetArray (Var "foldVal")))
        (Let "value" (SelectElem 1 (GetArray (Var "foldVal"))) foldFuncBody)))
        (FoldValue "foldFunc" (Const $ DocumentValue Document { getFields = [] }) (Var "x")))

-- Compiles the projection to a core language expression that returns the result of of applying the
-- projection to the variable 'ROOT'.
desugarInclusionProjection :: InclusionProjectionNode -> Either Error (CoreExpr Document)
desugarInclusionProjection (InclusionProjectionNode inclusions) = do
    func <- makeProjectSubtreeFunc (InclusionProjectionNode inclusions)
    return $ GetDocument $ FunctionDef "projFunc" func (FunctionApp "projFunc" [Var "ROOT"])

-- Evaluates an inclusion projection, returning either the resulting document, or an error.
evalInclusionProjection :: InclusionProjectionNode -> Document -> Either Error Document
evalInclusionProjection proj doc = do
    desugaredProj <- desugarInclusionProjection proj
    evalCoreExpr desugaredProj Environment {
        boundVariables = [("ROOT", DocumentValue doc)],
        definedFunctions = [] }
