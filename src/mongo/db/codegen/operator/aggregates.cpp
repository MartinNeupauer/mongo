#include "mongo/db/codegen/operator/aggregates.h"

namespace rohan
{
    boost::intrusive_ptr<anta::Statement> Aggregates::generateCountInit(
        anta::ExprPlaceholders* env,
        unsigned offset,
        const std::vector<boost::intrusive_ptr<const anta::Expr>>&)
    {
        fragment_();
            hole_(env, offset) = const64_(1);
        return endfragment_();
    }

    boost::intrusive_ptr<anta::Statement> Aggregates::generateCountStep(
        anta::ExprPlaceholders* env,
        unsigned offset,
        const std::vector<boost::intrusive_ptr<const anta::Expr>>&)
    {
        fragment_();
            hole_(env, offset) = hole_(env, offset) + const64_(1);
        return endfragment_();
    }    

    boost::intrusive_ptr<anta::Statement> Aggregates::generateGenericAggFinal(
        anta::ExprPlaceholders* envOut,
        unsigned offsetOut,
        anta::ExprPlaceholders* envTable,
        unsigned offsetTable)
    {
        fragment_();
            hole_(envOut, offsetOut) = hole_(envTable, offsetTable);
        return endfragment_();
    }
}