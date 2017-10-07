#include "mongo/db/codegen/operator/common.h"
#include "mongo/db/codegen/operator/collection_scan.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/operation_context.h"

#include <type_traits>

namespace rohan
{
    namespace runtime
    {
        struct CollectionScan
        {
            mongo::Record record;

            mongo::OperationContext* opCtx;
            const mongo::Collection* collection;
            std::unique_ptr<mongo::SeekableRecordCursor> cursor;

            static void open(CollectionScan* scan)
            {
                scan->cursor = scan->collection->getCursor(scan->opCtx, true);
            }

            static bool next(CollectionScan* scan)
            {
                if (auto record = scan->cursor->next()) {
                    scan->record = *record;
                    return true;
                }
                
                return false;
            }
        };
    }

    void CommonDeclarations::generateCollectionScan()
    {
        function_("cppScanOpen", (void*)runtime::CollectionScan::open);
            param_(pint8_, "%this");
        end_();

        function_(int1_, "cppScanNext", (void*)runtime::CollectionScan::next);
            param_(pint8_, "%this");
        end_();

        // Flatten the mongo::Record
        static_assert(std::is_standard_layout<mongo::Record>::value, "Record must have a standard layout");
        auto recordType = factory_.UStructType(factory_.globalScope(), "mongo::Record");
            recordType->addField(int64_, "id");
            recordType->addField(pint8_, "data");
            recordType->addField(int_, "size");
    }
    void XteCollectionScan::generate(GenContext& ctx)
    {
        // Generate the Open call
        {
            ctx.open_ = mfunction_(uniqueName("collScanOpen"));
                generateOpenParameters();
                generateOpenReturns();
            body_();
                auto collScanState = var_("%collScanState", var_("%state") + const_(stateOffset_));
                call_("cppScanOpen", { collScanState });

                mreturn_(rohan::kOpenDone);
            end_();
        }

        // Generate the GetNext call
        {
            auto recordType = factory_.GlobalType("mongo::Record");
            ctx.nextrow_ = mfunction_(uniqueName("collScanGetNext"));
                generateNextParameters();
                generateNextReturns();
            body_();
                auto columns = generateColumnVariables(outputSchema());
                auto collScanState = var_("%collScanState", var_("%state") + const_(stateOffset_));

                auto result = var_("%result", eval_("cppScanNext", { collScanState }));
                if_(!result);
                    mreturn_(rohan::kNextRowEOS);
                end_();

                auto record = var_("%record", cast_(ptr_(recordType),collScanState));
                columns[0] = record["data"];

                auto field = var_("%field", eval_("BSON::getField", { columns[0], const_("_id")}));
                if_ (field);
                    call_("printString", {const_("found")});
                else_();
                    call_("printString", {const_("not found")});
                end_();

                call_("printLn", {});
                mreturn_(rohan::kNextRowDone);
            end_();
        }
    }

    void XteCollectionScan::calculateRuntimeState(RuntimeState& state)
    {
        state._size = anta::alignUp(state._size, alignof(runtime::CollectionScan));
        stateOffset_ = state._size;
        state._size += sizeof(runtime::CollectionScan);

        auto offset = stateOffset_;
        auto collection = _params.collection;
        auto fwdConstruct = state._construct;
        state._construct = [offset, collection, fwdConstruct](char* buffer, mongo::OperationContext* ctx)
        {
            auto collState = new(buffer+offset) runtime::CollectionScan;

            collState->opCtx = ctx;
            collState->collection = collection;
            fwdConstruct(buffer, ctx);
        };

        auto fwdDestruct = state._destruct;
        state._destruct = [offset, fwdDestruct](char* buffer, mongo::OperationContext* ctx)
        {
            auto collState = reinterpret_cast<runtime::CollectionScan*>(buffer+offset);
            collState->~CollectionScan();

            fwdDestruct(buffer, ctx);
        };
    }
}