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
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/sorted_data_interface.h"

namespace mongo::sbe {
class IndexScanStage final : public PlanStage {
protected:
    void doSaveState() override;
    void doRestoreState() override;
    void doDetachFromOperationContext() override;
    void doAttachFromOperationContext(OperationContext* opCtx) override;

public:
    IndexScanStage(const NamespaceStringOrUUID& name,
                   std::string_view indexName,
                   boost::optional<value::SlotId> recordSlot,
                   boost::optional<value::SlotId> recordISlot,
                   const std::vector<std::string>& fields,
                   const std::vector<value::SlotId>& vars,
                   boost::optional<value::SlotId> seekKeySlotLow,
                   boost::optional<value::SlotId> seekKeySlotHi);

    std::unique_ptr<PlanStage> clone() final;

    void prepare(CompileCtx& ctx) final;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, value::SlotId slot) final;
    void open(bool reOpen) final;
    PlanState getNext() final;
    void close() final;

    std::vector<DebugPrinter::Block> debugPrint() final;

private:
    const NamespaceStringOrUUID _name;
    const std::string _indexName;
    const boost::optional<value::SlotId> _recordSlot;
    const boost::optional<value::SlotId> _recordIdSlot;
    const std::vector<std::string> _fields;
    const std::vector<value::SlotId> _vars;
    const boost::optional<value::SlotId> _seekKeySlotLow;
    const boost::optional<value::SlotId> _seekKeySlotHi;

    OperationContext* _opCtx{nullptr};
    std::unique_ptr<value::ViewOfValueAccessor> _recordAccessor;
    std::unique_ptr<value::ViewOfValueAccessor> _recordIdAccessor;

    std::map<std::string, std::unique_ptr<value::ViewOfValueAccessor>, std::less<>> _fieldAccessors;
    std::map<value::SlotId, value::SlotAccessor*, std::less<>> _varAccessors;

    value::SlotAccessor* _seekKeyLowAccessor{nullptr};
    value::SlotAccessor* _seekKeyHiAccessor{nullptr};

    KeyString::Value _startPoint;
    KeyString::Value* _seekKeyLow{nullptr};
    KeyString::Value* _seekKeyHi{nullptr};

    std::unique_ptr<SortedDataInterface::Cursor> _cursor;
    std::weak_ptr<const IndexCatalogEntry> _weakIndexCatalogEntry;
    boost::optional<AutoGetCollectionForRead> _coll;
    boost::optional<KeyStringEntry> _nextRecord;

    bool _firstGetNext{true};
};
}  // namespace mongo::sbe
