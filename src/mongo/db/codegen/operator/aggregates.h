#pragma once

#include "mongo/db/codegen/anta/Sema.h"
#include "mongo/db/codegen/anta/Generator.h"

namespace rohan
{
    class Aggregates : public anta::Generator
    {
    public:
        Aggregates(anta::SemaFactory& f) : anta::Generator(f) {}

        boost::intrusive_ptr<anta::Statement> generateCountInit(
            anta::ExprPlaceholders* env,
            unsigned offset,
            const std::vector<boost::intrusive_ptr<const anta::Expr>>&);
        boost::intrusive_ptr<anta::Statement> generateCountStep(
            anta::ExprPlaceholders* env,
            unsigned offset,
            const std::vector<boost::intrusive_ptr<const anta::Expr>>&);
        boost::intrusive_ptr<anta::Statement> generateGenericAggFinal(
            anta::ExprPlaceholders* envOut,
            unsigned offsetOut,
            anta::ExprPlaceholders* envTable,
            unsigned offsetTable);
        std::vector<const anta::Type*> getCountInternalTypes() const 
        {
            return {int64_};
        }
    };
}