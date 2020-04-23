/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "mongo/db/query/plan_executor_sbe.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/query/sbe_stage_builder.h"
#include "mongo/logv2/log.h"

namespace mongo {
StatusWith<std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>> PlanExecutor::make(
    OperationContext* opCtx,
    std::unique_ptr<CanonicalQuery> cq,
    std::pair<std::unique_ptr<sbe::PlanStage>, stage_builder::PlanStageData> root,
    NamespaceString nss,
    std::unique_ptr<PlanYieldPolicySBE> yieldPolicy) {

    LOGV2_DEBUG(47429003,
                5,
                "SBE plan: {slots} {stages}",
                "slots"_attr = root.second.debug(),
                "stages"_attr = sbe::DebugPrinter{}.print(root.first.get()));

    root.first->prepare(root.second.ctx);

    auto execImpl = new PlanExecutorSBE(
        opCtx, std::move(cq), std::move(root), nss, false, boost::none, std::move(yieldPolicy));
    PlanExecutor::Deleter planDeleter(opCtx);

    std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> exec(execImpl, std::move(planDeleter));
    return std::move(exec);
}

StatusWith<std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>> PlanExecutor::make(
    OperationContext* opCtx,
    std::unique_ptr<CanonicalQuery> cq,
    std::pair<std::unique_ptr<sbe::PlanStage>, stage_builder::PlanStageData> root,
    NamespaceString nss,
    std::queue<std::pair<BSONObj, boost::optional<RecordId>>> stash,
    std::unique_ptr<PlanYieldPolicySBE> yieldPolicy) {

    LOGV2_DEBUG(47429004,
                5,
                "SBE plan: {slots} {stages}",
                "slots"_attr = root.second.debug(),
                "stages"_attr = sbe::DebugPrinter{}.print(root.first.get()));

    auto execImpl = new PlanExecutorSBE(
        opCtx, std::move(cq), std::move(root), nss, true, stash, std::move(yieldPolicy));
    PlanExecutor::Deleter planDeleter(opCtx);

    std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> exec(execImpl, std::move(planDeleter));
    return std::move(exec);
}

PlanExecutorSBE::PlanExecutorSBE(
    OperationContext* opCtx,
    std::unique_ptr<CanonicalQuery> cq,
    std::pair<std::unique_ptr<sbe::PlanStage>, stage_builder::PlanStageData> root,
    NamespaceString nss,
    bool isOpen,
    boost::optional<std::queue<std::pair<BSONObj, boost::optional<RecordId>>>> stash,
    std::unique_ptr<PlanYieldPolicySBE> yieldPolicy)
    : _state{isOpen ? State::opened : State::beforeOpen},
      _opCtx(opCtx),
      _nss(nss),
      _root(std::move(root.first)),
      _cq{std::move(cq)},
      _yieldPolicy(std::move(yieldPolicy)) {
    invariant(_root);

    auto&& data = root.second;

    if (data.resultSlot) {
        _result = _root->getAccessor(data.ctx, *data.resultSlot);
        uassert(ErrorCodes::InternalError, "Query does not have result slot.", _result);
    }

    if (data.recordIdSlot) {
        _resultRecordId = _root->getAccessor(data.ctx, *data.recordIdSlot);
        uassert(ErrorCodes::InternalError, "Query does not have recordId slot.", _resultRecordId);
    }

    if (data.oplogTsSlot) {
        _oplogTs = _root->getAccessor(data.ctx, *data.oplogTsSlot);
        uassert(ErrorCodes::InternalError, "Query does not have oplogTs slot.", _oplogTs);
    }

    _shouldTrackLatestOplogTimestamp = data.shouldTrackLatestOplogTimestamp;
    _shouldTrackResumeToken = data.shouldTrackResumeToken;

    if (!isOpen) {
        _root->attachFromOperationContext(_opCtx);
    }

    if (stash) {
        _stash = std::move(*stash);
    }

    // Callers are allowed to disable yielding for this plan by passing a null yield policy.
    if (_yieldPolicy) {
        _yieldPolicy->setRootStage(_root.get());
    }
}

void PlanExecutorSBE::saveState() {
    invariant(_root);
    _root->saveState();
}

void PlanExecutorSBE::restoreState() {
    invariant(_root);
    _root->restoreState();
}

void PlanExecutorSBE::detachFromOperationContext() {
    invariant(_opCtx);
    invariant(_root);
    _root->detachFromOperationContext();
    _opCtx = nullptr;
}

void PlanExecutorSBE::reattachToOperationContext(OperationContext* opCtx) {
    invariant(!_opCtx);
    invariant(_root);
    _root->attachFromOperationContext(opCtx);
    _opCtx = opCtx;
}

void PlanExecutorSBE::markAsKilled(Status killStatus) {
    invariant(!killStatus.isOK());
    // If killed multiple times, only retain the first status.
    if (_killStatus.isOK()) {
        _killStatus = killStatus;
    }
}

void PlanExecutorSBE::dispose(OperationContext* opCtx) {
    if (_root && _state != State::beforeOpen) {
        _root->close();
        _state = State::beforeOpen;
    }

    _root.reset();
}

void PlanExecutorSBE::enqueue(const Document& obj) {
    enqueue(obj.toBson());
}

void PlanExecutorSBE::enqueue(const BSONObj& obj) {
    invariant(_state == State::opened);
    _stash.push({obj.getOwned(), boost::none});
}

PlanExecutor::ExecState PlanExecutorSBE::getNext(Document* objOut, RecordId* dlOut) {
    invariant(_root);

    BSONObj obj;
    auto result = getNext(&obj, dlOut);
    if (result == PlanExecutor::ExecState::ADVANCED) {
        *objOut = Document{std::move(obj)};
    }
    return result;
}

PlanExecutor::ExecState PlanExecutorSBE::getNext(BSONObj* out, RecordId* dlOut) {
    invariant(_root);

    if (!_stash.empty()) {
        auto&& [doc, recordId] = _stash.front();
        _stash.pop();
        *out = std::move(doc);
        if (dlOut && recordId) {
            *dlOut = *recordId;
        }
        return PlanExecutor::ExecState::ADVANCED;
    } else if (_root->getCommonStats()->isEOF) {
        _root->close();
        _state = State::beforeOpen;
        return PlanExecutor::ExecState::IS_EOF;
    }

    if (_state == State::beforeOpen) {
        _state = State::opened;
        _root->open(false);
    }

    invariant(_state == State::opened);

    auto result = fetchNext(_root.get(), _result, _resultRecordId, out, dlOut);
    if (result == sbe::PlanState::IS_EOF) {
        _root->close();
        _state = State::beforeOpen;
        return PlanExecutor::ExecState::IS_EOF;
    }
    invariant(result == sbe::PlanState::ADVANCED);
    return PlanExecutor::ExecState::ADVANCED;
}

Timestamp PlanExecutorSBE::getLatestOplogTimestamp() const {
    if (_shouldTrackLatestOplogTimestamp) {
        invariant(_oplogTs);

        auto [tag, val] = _oplogTs->getViewOfValue();
        uassert(ErrorCodes::InternalError,
                "Collection scan was asked to track latest operation time, "
                "but found a result without a valid 'ts' field",
                tag == sbe::value::TypeTags::Timestamp);
        return Timestamp{sbe::value::bitcastTo<uint64_t>(val)};
    }
    return {};
}

BSONObj PlanExecutorSBE::getPostBatchResumeToken() const {
    if (_shouldTrackResumeToken) {
        invariant(_resultRecordId);

        auto [tag, val] = _resultRecordId->getViewOfValue();
        uassert(ErrorCodes::InternalError,
                "Collection scan was asked to track resume token, "
                "but found a result without a valid RecordId",
                tag == sbe::value::TypeTags::NumberInt64);
        return BSON("$recordId" << sbe::value::bitcastTo<int64_t>(val));
    }
    return {};
}

sbe::PlanState fetchNext(sbe::PlanStage* root,
                         sbe::value::SlotAccessor* resultSlot,
                         sbe::value::SlotAccessor* recordIdSlot,
                         BSONObj* out,
                         RecordId* dlOut) {
    invariant(out);

    auto state = root->getNext();
    if (state == sbe::PlanState::IS_EOF) {
        return state;
    }
    invariant(state == sbe::PlanState::ADVANCED);

    // copy the result
    if (resultSlot) {
        auto [tag, val] = resultSlot->getViewOfValue();
        if (tag == sbe::value::TypeTags::Object) {
            BSONObjBuilder bb;
            sbe::bson::convertToBsonObj(bb, sbe::value::getObjectView(val));
            *out = bb.obj();
        } else if (tag == sbe::value::TypeTags::bsonObject) {
            *out = BSONObj(sbe::value::bitcastTo<const char*>(val));
        } else {
            // The query is supposed to return an object.
            MONGO_UNREACHABLE;
        }
    }

    if (dlOut) {
        invariant(recordIdSlot);
        auto [tag, val] = recordIdSlot->getViewOfValue();
        if (tag == sbe::value::TypeTags::NumberInt64) {
            *dlOut = RecordId{sbe::value::bitcastTo<int64_t>(val)};
        }
    }
    return state;
}

}  // namespace mongo
