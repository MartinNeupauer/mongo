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

#pragma once

#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/db/query/plan_executor.h"

namespace mongo {
class PlanExecutorSBE final : public PlanExecutor {
public:
    PlanExecutorSBE(OperationContext* opCtx,
                    std::unique_ptr<CanonicalQuery> cq,
                    std::unique_ptr<sbe::PlanStage> root,
                    sbe::value::SlotAccessor* result,
                    sbe::value::SlotAccessor* resultRecordId,
                    NamespaceString nss);

    WorkingSet* getWorkingSet() const override {
        MONGO_UNREACHABLE;
    }

    PlanStage* getRootStage() const override {
        return nullptr;
    }

    CanonicalQuery* getCanonicalQuery() const override {
        return _cq.get();
    }

    const NamespaceString& nss() const override {
        return _nss;
    }

    OperationContext* getOpCtx() const override {
        return _opCtx;
    }

    const boost::intrusive_ptr<ExpressionContext>& getExpCtx() const override {
        static boost::intrusive_ptr<ExpressionContext> unused;
        return unused;
    }

    void saveState();
    void restoreState();

    void detachFromOperationContext();
    void reattachToOperationContext(OperationContext* opCtx);

    void restoreStateWithoutRetrying() override {
        MONGO_UNREACHABLE;
    }

    ExecState getNextSnapshotted(Snapshotted<Document>* objOut, RecordId* dlOut) override {
        MONGO_UNREACHABLE;
    }

    ExecState getNextSnapshotted(Snapshotted<BSONObj>* objOut, RecordId* dlOut) override {
        MONGO_UNREACHABLE;
    }

    ExecState getNext(Document* objOut, RecordId* dlOut) override;
    ExecState getNext(BSONObj* out, RecordId* dlOut) override;

    bool isEOF() override {
        return _state == State::beforeOpen;
    }

    Status executePlan() override {
        MONGO_UNREACHABLE;
    }

    void markAsKilled(Status killStatus);

    void dispose(OperationContext* opCtx);

    void enqueue(const Document& obj);
    void enqueue(const BSONObj& obj);

    bool isMarkedAsKilled() const override {
        return !_killStatus.isOK();
    }

    Status getKillStatus() override {
        invariant(isMarkedAsKilled());
        return _killStatus;
    }

    bool isDisposed() const override {
        return !_root;
    }

    bool isDetached() const override {
        return !_opCtx;
    }

    Timestamp getLatestOplogTimestamp() const override {
        MONGO_UNREACHABLE;
    }

    BSONObj getPostBatchResumeToken() const override {
        return {};
    }

    Status getMemberObjectStatus(const Document& memberObj) const override {
        MONGO_UNREACHABLE;
    }

    Status getMemberObjectStatus(const BSONObj& memberObj) const override {
        MONGO_UNREACHABLE;
    }

private:
    enum State { beforeOpen, opened, stashed };

    State _state{beforeOpen};

    OperationContext* _opCtx;

    NamespaceString _nss;

    std::unique_ptr<sbe::PlanStage> _root;
    sbe::value::SlotAccessor* _result{nullptr};
    sbe::value::SlotAccessor* _resultRecordId{nullptr};

    BSONObj _stash;

    // If _killStatus has a non-OK value, then we have been killed and the value represents the
    // reason for the kill.
    Status _killStatus = Status::OK();

    std::unique_ptr<CanonicalQuery> _cq;
};
}  // namespace mongo
