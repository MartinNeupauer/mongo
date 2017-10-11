/**
 *    Copyright (C) 2013 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/codegen/operator/common.h"
#include "mongo/db/codegen/operator/collection_scan.h"
#include "mongo/db/codegen/operator/project.h"
#include "mongo/db/codegen/operator/aggregates.h"
#include "mongo/db/codegen/operator/hash_aggregation.h"

#include "mongo/db/exec/collection_scan.h"
#include "mongo/db/exec/pipeline_codegen.h"
#include "mongo/db/exec/scoped_timer.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/pipeline/document_source_cursor.h"
#include "mongo/db/pipeline/document_source_group.h"
#include "mongo/stdx/memory.h"

#include "mongo/util/log.h"

// This is a horrible hack - mongo defines 'verify' macro (uggrh) that clashes with LLVM methods 'verify'
#ifdef verify
#undef verify
#endif
#include "mongo/db/codegen/machine/jitter.h"

#include <string>

namespace anta
{
    unsigned theTestFunction(machine::Jitter& jitter, rohan::NativeOpenFunction& openFn, rohan::NativeGetNextFunction& getNextFn);
    rohan::RuntimeState generateNative(
        SemaFactory& f, 
        machine::Jitter& jitter,
        const std::unique_ptr<rohan::XteOperator>& root,
        rohan::NativeOpenFunction& openFn,
        rohan::NativeGetNextFunction& getNextFn);
}

namespace mongo {

using std::unique_ptr;
using std::vector;
using stdx::make_unique;

// static
const char* CodeGenStage::kStageType = "CODEGEN";

CodeGenStage::CodeGenStage(OperationContext* opCtx, WorkingSet* workingSet) : PlanStage(kStageType, opCtx), _workingSet(workingSet) {}

CodeGenStage::~CodeGenStage() {}

bool CodeGenStage::isEOF() {
    return true;
}

bool CodeGenStage::translate(Pipeline* pipeline)
{
    anta::SemaFactory f;
    rohan::CommonDeclarations common(f);
    common.generate();
    
    const auto& sources = pipeline->getSources();

    unique_ptr<rohan::XteOperator> xteroot;
    bool failToCompile = false;

    for(const auto& source : sources) {
        if (auto cursor = dynamic_cast<const DocumentSourceCursor*>(source.get()))
        {
            auto exec = cursor->getExecutor();
            auto root = exec->getRootStage();

            if (!root) {
                throw std::logic_error("!!!!");
            }

            if (auto scan = dynamic_cast<CollectionScan*>(root))
            {
                auto& params = scan->params();
                auto collection = params.collection;

                if (!collection) {
                    throw std::logic_error("!!!!");
                }

                xteroot = make_unique<rohan::XteCollectionScan>(f, params);
            }
        } else if (auto groupby = dynamic_cast<const DocumentSourceGroup*>(source.get())) {
            auto& idexprs = groupby->getIdExpressions();
            vector<StringData> groupByCols;
            if (idexprs.size() == 1 && dynamic_cast<const ExpressionConstant*>(idexprs[0].get())) {
                // no group by columns
            } else {
                for(auto& col : idexprs) {
                    auto fieldAccess = dynamic_cast<const ExpressionFieldPath*>(col.get());
                    if (!fieldAccess) {
                        failToCompile = true;
                        break;
                    }

                    auto& fieldPath = fieldAccess->getFieldPath();
                    if (fieldPath.getPathLength() != 2)
                    {
                        failToCompile = true;
                        break;
                    }
                    groupByCols.push_back(fieldPath.getFieldName(1));
                }
                if (!failToCompile) {
                    // input environment
                    auto envIn = f.createPlaceholders(xteroot->outputSchema());

                    // output environment
                    rohan::SchemaType outTypes(groupByCols.size(), f.globalScope()->getType("BSONVariant"));
                    auto envOut = f.createPlaceholders(outTypes);

                    vector<boost::intrusive_ptr<anta::Statement>> stmts;

                    for(unsigned idx=0; idx<groupByCols.size(); ++idx) {
                        stmts.push_back(
                            f.Assign(
                                f.Hole(envOut, idx),
                                f.FuncExpr(f.GlobalFunction("BSON::getVariantFromDocument"), {f.Hole(envIn, 0), f.UStringConst(groupByCols[idx].toString())})
                            )
                        );
                    }

                    xteroot = make_unique<rohan::XteProject>(f,std::move(xteroot),envIn,envOut,f.Stmts(std::move(stmts)));
                }
            }

            rohan::Aggregates aggs(f);
            auto gbsize = groupByCols.size();
            auto internalTypes = aggs.getCountInternalTypes();

            auto envIn = f.createPlaceholders(xteroot->outputSchema());

            std::vector<const anta::Type*> groupbyTypes(gbsize,f.globalScope()->getType("BSONVariant"));

            groupbyTypes.insert(groupbyTypes.end(), internalTypes.begin(), internalTypes.end());

            auto envOut = f.createPlaceholders(groupbyTypes); // hack

            // internal env
            auto envTableOpen = f.createPlaceholders(groupbyTypes); // hack
            auto envTableGetRow = f.createPlaceholders(groupbyTypes); //hack

            auto initGroup = aggs.generateCountInit(envTableOpen, gbsize, {});
            auto aggStep = aggs.generateCountStep(envTableOpen, gbsize, {});
            auto finalStep = aggs.generateGenericAggFinal(envOut, gbsize, envTableGetRow, gbsize);

            std::vector<unsigned> groupbyCols;
            for(unsigned i=0;i<gbsize;++i) groupbyCols.push_back(i);
            
            xteroot = make_unique<rohan::XteHashAgg>(f, std::move(xteroot), groupbyCols, envIn, envOut, envTableOpen, envTableGetRow, initGroup, aggStep, finalStep);
        }
    }

    if (!xteroot || failToCompile)
        return false;

    _jitter = make_unique<machine::Jitter>();

    _state = anta::generateNative(f, *_jitter, xteroot, _openFunction, _getNextFunction);
    _stateBuffer.resize(_state._size);

    return true;
}

PlanStage::StageState CodeGenStage::doWork(WorkingSetID* out) {
    if (_first) {
        _first = false;

        _state._construct(_stateBuffer.data(), getOpCtx());

        int openError{0};
        rohan::OpenCallResult openResult = _openFunction(_stateBuffer.data(), &openError);

        if (openResult == rohan::OpenCallResult::kError) {
            Status status(
                ErrorCodes::InternalError,
                str::stream()
                    << "Native Open function returned error code: "
                    << openError);
            *out = WorkingSetCommon::allocateStatusMember(_workingSet, status);
            return PlanStage::FAILURE;
        } else if (openResult == rohan::OpenCallResult::kCancel) {
            Status status(
                ErrorCodes::InternalError,
                str::stream()
                    << "Native Open function was canceled");
            *out = WorkingSetCommon::allocateStatusMember(_workingSet, status);
            return PlanStage::FAILURE;
        }
    }

    int getNextError{0};
    rohan::GetNextCallResult getNextResult = _getNextFunction(_stateBuffer.data(), &getNextError);

    switch(getNextResult)
    {
        case rohan::GetNextCallResult::kOK:
        {
            *out = _workingSet->allocate();
            auto member = _workingSet->get(*out);

            BSONObjBuilder bob;
            bob.append("a",0);
            member->obj = Snapshotted<BSONObj>(SnapshotId(),bob.obj());
            member->transitionToOwnedObj();
            return PlanStage::ADVANCED;
        }
        case rohan::GetNextCallResult::kEOS:
        {
            _state._destruct(_stateBuffer.data(), getOpCtx());
            return PlanStage::IS_EOF;
        }
        case rohan::GetNextCallResult::kError:
        {
            Status status(
                ErrorCodes::InternalError,
                str::stream()
                    << "Native GetNext function returned error code: "
                    << getNextError);
            *out = WorkingSetCommon::allocateStatusMember(_workingSet, status);
            return PlanStage::FAILURE;
        }
        case rohan::GetNextCallResult::kCancel:
        {
            Status status(
                ErrorCodes::InternalError,
                str::stream()
                    << "Native GetNext function was canceled");
            *out = WorkingSetCommon::allocateStatusMember(_workingSet, status);
            return PlanStage::FAILURE;
        }
    }

    // Unreachable
    return PlanStage::IS_EOF;
}

unique_ptr<PlanStageStats> CodeGenStage::getStats() {
    _commonStats.isEOF = isEOF();
    return make_unique<PlanStageStats>(_commonStats, STAGE_CODEGEN);
}

const SpecificStats* CodeGenStage::getSpecificStats() const {
    return nullptr;
}

}  // namespace mongo
