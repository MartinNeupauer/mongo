#include "mongo/db/codegen/operator/collection_scan.h"

namespace rohan
{
    namespace runtime
    {
        struct CollectionScan
        {
            void* _placeholder;
        };
    }

    void XteCollectionScan::generate(GenContext& ctx)
    {
        // Generate the Open call
        {
            ctx.open_ = mfunction_(uniqueName("collScanOpen"));
                generateOpenParameters();
                generateOpenReturns();
            body_();
                call_("printString", {const_("collection scan open called")});
                call_("printLn", {});
                auto collScanState = var_("%collScanState", var_("%state") + const_(stateOffset_));
                //call_(cppOpen, { var_("%state"), collScanState, const_(_collToken) });

                mreturn_(rohan::kOpenDone);
            end_();
        }

        // Generate the GetNext call
        {
            ctx.nextrow_ = mfunction_(uniqueName("collScanGetNext"));
                generateNextParameters();
                generateNextReturns();
            body_();
                call_("printString", {const_("collection scan getnext called")});
                call_("printLn", {});
            //auto collScanState = var_("%collScanState", cast_(ptr_(stateType), var_("%state") + const_(stateOffset_)));
    
                mreturn_(rohan::kNextRowEOS);
            end_();
        }
    }

    void XteCollectionScan::calculateStateOffset(unsigned& offset)
    {
        offset = anta::alignUp(offset, alignof(runtime::CollectionScan));
        stateOffset_ = offset;
        offset += sizeof(runtime::CollectionScan);
    }
}