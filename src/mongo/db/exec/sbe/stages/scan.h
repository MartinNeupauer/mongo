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

#include "mongo/db/db_raii.h"
#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/storage/record_store.h"

namespace mongo {
namespace sbe {
class ScanStage final : public PlanStage {
protected:
    void doSaveState() override;
    void doRestoreState() override;
    void doDetachFromOperationContext() override;
    void doAttachFromOperationContext(OperationContext* opCtx) override;

public:
    ScanStage(const NamespaceStringOrUUID& name,
              std::string_view recordName,
              std::string_view recordIdName,
              const std::vector<std::string>& fields,
              const std::vector<std::string>& varNames,
              std::string_view seekKeyName);

    std::unique_ptr<PlanStage> clone() override;

    void prepare(CompileCtx& ctx) override;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, std::string_view field) override;
    void open(bool reOpen) override;
    PlanState getNext() override;
    void close() override;

    std::vector<DebugPrinter::Block> debugPrint() override;

private:
    const NamespaceStringOrUUID _name;
    const std::string _recordName;
    const std::string _recordIdName;
    const std::vector<std::string> _fields;
    const std::vector<std::string> _varNames;
    const std::string _seekKeyName;

    OperationContext* _opCtx{nullptr};
    std::unique_ptr<value::ViewOfValueAccessor> _recordAccessor;
    std::unique_ptr<value::ViewOfValueAccessor> _recordIdAccessor;

    std::map<std::string, std::unique_ptr<value::ViewOfValueAccessor>, std::less<>> _fieldAccessors;
    std::map<std::string, value::SlotAccessor*, std::less<>> _varAccessors;
    value::SlotAccessor* _seekKeyAccessor{nullptr};

    std::unique_ptr<SeekableRecordCursor> _cursor;
    boost::optional<AutoGetCollectionForRead> _coll;
    RecordId _key;
    bool _firstGetNext{false};
};

class ParallelScanStage final : public PlanStage {
    struct Range {
        RecordId begin;
        RecordId end;
    };
    struct ParallelState {
        Mutex mutex = MONGO_MAKE_LATCH("ParallelScanStage::ParallelState::mutex");
        std::vector<Range> ranges;
        std::atomic<size_t> currentRange{0};
    };

    boost::optional<Record> nextRange();
    bool needsRange() const {
        return _currentRange == std::numeric_limits<std::size_t>::max();
    }
    void setNeedsRange() {
        _currentRange = std::numeric_limits<std::size_t>::max();
    }

protected:
    void doSaveState() override;
    void doRestoreState() override;
    void doDetachFromOperationContext() override;
    void doAttachFromOperationContext(OperationContext* opCtx) override;

public:
    ParallelScanStage(const NamespaceStringOrUUID& name,
                      std::string_view recordName,
                      std::string_view recordIdName,
                      const std::vector<std::string>& fields,
                      const std::vector<std::string>& varNames);

    ParallelScanStage(const std::shared_ptr<ParallelState>& state,
                      const NamespaceStringOrUUID& name,
                      std::string_view recordName,
                      std::string_view recordIdName,
                      const std::vector<std::string>& fields,
                      const std::vector<std::string>& varNames);

    std::unique_ptr<PlanStage> clone() override;

    void prepare(CompileCtx& ctx) override;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, std::string_view field) override;
    void open(bool reOpen) override;
    PlanState getNext() override;
    void close() override;

    std::vector<DebugPrinter::Block> debugPrint() override;

private:
    const NamespaceStringOrUUID _name;
    const std::string _recordName;
    const std::string _recordIdName;
    const std::vector<std::string> _fields;
    const std::vector<std::string> _varNames;

    std::shared_ptr<ParallelState> _state;

    OperationContext* _opCtx{nullptr};

    std::unique_ptr<value::ViewOfValueAccessor> _recordAccessor;
    std::unique_ptr<value::ViewOfValueAccessor> _recordIdAccessor;

    std::map<std::string, std::unique_ptr<value::ViewOfValueAccessor>, std::less<>> _fieldAccessors;
    std::map<std::string, value::SlotAccessor*, std::less<>> _varAccessors;

    size_t _currentRange{std::numeric_limits<std::size_t>::max()};
    Range _range;

    std::unique_ptr<SeekableRecordCursor> _cursor;
    boost::optional<AutoGetCollectionForRead> _coll;
};
}  // namespace sbe
}  // namespace mongo