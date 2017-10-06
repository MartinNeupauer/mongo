#pragma once
#include "mongo/db/codegen/operator/operator.h"

namespace rohan
{
    class XteCollectionScan : public XteOperator
    {
    public:
        XteCollectionScan(anta::SemaFactory& f) : XteOperator(f, {}) {}

        virtual void generate(GenContext&) override;
        virtual void calculateStateOffset(unsigned&) override;
    };
}
