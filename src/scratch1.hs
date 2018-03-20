import Data.Void

type Array = [Variant]

data Bool3VL = Unknown3VL | False3VL | True3VL deriving (Eq, Show)

data Variant =
    MkNull |
    MkInt Int |
    MkStr String |
    MkVarDoc Document |
    MkVarArray Array
    deriving (Eq, Show)

-- Nulls do not compare equal to anything
cmpEq MkNull _ = False
cmpEq _ MkNull = False
-- lhs and rhs types must be the same
cmpEq (MkInt lhs) (MkInt rhs) = lhs == rhs
cmpEq (MkVarDoc lhs) (MkVarDoc rhs) = lhs == rhs
cmpEq (MkVarArray lhs) (MkVarArray rhs) = lhs == rhs
-- anything else if false
cmpEq _ _ = False


allElemMatch::Array->Condition->Bool
allElemMatch a c = False

anyElemMatch::Array->Condition->Bool
anyElemMatch a c = False
-- does the law of excluded middle apply? in the presense of nulls
-- i.e. allElemMatch a c <=> noElemMatch a (not c) ?

newtype Document =
    MkDoc [(String, Variant)]
    deriving (Eq, Show)

data Condition =
    And Condition Condition

doubleMe x = x + x
