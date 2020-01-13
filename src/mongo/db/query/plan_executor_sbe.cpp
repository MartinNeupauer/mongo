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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/db/query/plan_executor_sbe.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/query/sbe_stage_builder.h"
#include "mongo/util/log.h"

namespace mongo {
StatusWith<std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>> PlanExecutor::make(
    OperationContext* opCtx,
    std::unique_ptr<CanonicalQuery> cq,
    std::unique_ptr<sbe::PlanStage> root,
    NamespaceString nss) {

    LOG(3) << "SBE plan:\n" << sbe::DebugPrinter{}.print(root.get());

    sbe::CompileCtx ctx;
    root->prepare(ctx);
    auto resultSlot = root->getAccessor(ctx, sbe::value::SystemSlots::kResultSlot);
    uassert(ErrorCodes::InternalError, "Query does not have result slot.", resultSlot);

    sbe::value::SlotAccessor* resultRecordId{nullptr};
    if (cq && cq->metadataDeps()[DocumentMetadataFields::kRecordId]) {
        resultRecordId = root->getAccessor(ctx, sbe::value::SystemSlots::kRecordIdSlot);
        uassert(ErrorCodes::InternalError, "Query does not have record ID slot.", resultRecordId);
    }

    auto execImpl =
        new PlanExecutorSBE(opCtx, std::move(cq), std::move(root), resultSlot, resultRecordId, nss);
    PlanExecutor::Deleter planDeleter(opCtx);

    std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> exec(execImpl, std::move(planDeleter));
    return std::move(exec);
}

PlanExecutorSBE::PlanExecutorSBE(OperationContext* opCtx,
                                 std::unique_ptr<CanonicalQuery> cq,
                                 std::unique_ptr<sbe::PlanStage> root,
                                 sbe::value::SlotAccessor* result,
                                 sbe::value::SlotAccessor* resultRecordId,
                                 NamespaceString nss)
    : _opCtx(opCtx),
      _nss(nss),
      _root(std::move(root)),
      _result(result),
      _resultRecordId(resultRecordId),
      _cq{std::move(cq)} {
    invariant(_root);
    invariant(_result);

    _root->attachFromOperationContext(_opCtx);
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
    _stash = obj.getOwned();
    _state = State::stashed;
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

    if (_state == State::beforeOpen) {
        _state = State::opened;
        _root->open(false);
    }

    if (_state == State::stashed) {
        *out = std::move(_stash);
        _state = State::opened;
        return PlanExecutor::ExecState::ADVANCED;
    }

    invariant(_state == State::opened);

    auto result = _root->getNext();

    if (result == sbe::PlanState::IS_EOF) {
        _root->close();
        _state = State::beforeOpen;
        return PlanExecutor::ExecState::IS_EOF;
    }
    invariant(result == sbe::PlanState::ADVANCED);

    // copy the result
    auto [tag, val] = _result->getViewOfValue();

    if (tag == sbe::value::TypeTags::Object) {
        BSONObjBuilder bb;
        sbe::bson::convertToBsonObj(bb, sbe::value::getObjectView(val));
        *out = bb.obj();
    } else if (tag == sbe::value::TypeTags::bsonObject) {
        *out = BSONObj(sbe::value::bitcastTo<const char*>(val));
    } else {
        // The query is supposed to return an object.
    }

    if (dlOut) {
        invariant(_resultRecordId);
        auto [tag, val] = _resultRecordId->getViewOfValue();
        if (tag == sbe::value::TypeTags::NumberInt64) {
            *dlOut = RecordId{sbe::value::bitcastTo<int64_t>(val)};
        }
    }
    return PlanExecutor::ExecState::ADVANCED;
}
}  // namespace mongo
