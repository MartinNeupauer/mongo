#pragma once
#include "mongo/db/codegen/operator/operator.h"
#include "mongo/db/exec/collection_scan_common.h"

namespace rohan
{
    class XteCollectionScan : public XteOperator
    {
        mongo::CollectionScanParams _params;
    public:
        XteCollectionScan(anta::SemaFactory& f, const mongo::CollectionScanParams& params) : XteOperator(f, {f.UPtrToInt8Type()}), _params(params) {}

        virtual void generate(GenContext&) override;
        virtual void calculateRuntimeState(RuntimeState&) override;
    };
}
