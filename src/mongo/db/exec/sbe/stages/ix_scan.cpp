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

#include "mongo/db/exec/sbe/stages/ix_scan.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/index/index_access_method.h"

namespace mongo {
namespace sbe {
IndexScanStage::IndexScanStage(const NamespaceStringOrUUID& name,
                               std::string_view indexName,
                               std::string_view recordName,
                               std::string_view recordIdName,
                               const std::vector<std::string>& fields,
                               const std::vector<std::string>& varNames)
    : _name(name),
      _indexName(indexName),
      _recordName(recordName),
      _recordIdName(recordIdName),
      _fields(fields),
      _varNames(varNames) {
    invariant(_fields.size() == _varNames.size());
}

std::unique_ptr<PlanStage> IndexScanStage::clone() {
    return std::make_unique<IndexScanStage>(
        _name, _indexName, _recordName, _recordIdName, _fields, _varNames);
}

void IndexScanStage::prepare(CompileCtx& ctx) {
    if (!_recordName.empty()) {
        _recordAccessor = std::make_unique<value::ViewOfValueAccessor>();
    }

    if (!_recordIdName.empty()) {
        _recordIdAccessor = std::make_unique<value::ViewOfValueAccessor>();
    }

    for (size_t idx = 0; idx < _fields.size(); ++idx) {
        auto [it, inserted] =
            _fieldAccessors.emplace(_fields[idx], std::make_unique<value::ViewOfValueAccessor>());
        uassert(ErrorCodes::InternalError,
                str::stream() << "duplicate field: " << _fields[idx],
                inserted);
        auto [itRename, insertedRename] = _varAccessors.emplace(_varNames[idx], it->second.get());
        uassert(ErrorCodes::InternalError,
                str::stream() << "duplicate field: " << _varNames[idx],
                insertedRename);
    }
}

value::SlotAccessor* IndexScanStage::getAccessor(CompileCtx& ctx, std::string_view field) {
    if (!_recordName.empty() && _recordName == field) {
        return _recordAccessor.get();
    }

    if (!_recordIdName.empty() && _recordIdName == field) {
        return _recordAccessor.get();
    }

    if (auto it = _varAccessors.find(field); it != _varAccessors.end()) {
        return it->second;
    }

    return ctx.getAccessor(field);
}

void IndexScanStage::doSaveState() {
    if (_cursor) {
        _cursor->save();
    }
}
void IndexScanStage::doRestoreState() {
    if (_cursor) {
        _cursor->restore();
    }
}

void IndexScanStage::doDetachFromOperationContext() {
    invariant(_opCtx);
    if (_cursor) {
        _cursor->detachFromOperationContext();
    }
    _opCtx = nullptr;
}
void IndexScanStage::doAttachFromOperationContext(OperationContext* opCtx) {
    invariant(opCtx);
    invariant(!_opCtx);
    if (_cursor) {
        _cursor->reattachToOperationContext(opCtx);
    }
    _opCtx = opCtx;
}

void IndexScanStage::open(bool reOpen) {
    invariant(_opCtx);
    if (!reOpen) {
        invariant(!_cursor);
        invariant(!_coll);
        _coll.emplace(_opCtx, _name);
    } else {
        invariant(_cursor);
        invariant(_coll);
    }

    _cursor.reset();
    auto collection = _coll->getCollection();

    if (collection) {
        auto indexCatalog = collection->getIndexCatalog();
        auto indexDesc = indexCatalog->findIndexByName(_opCtx, _indexName);
        if (indexDesc) {
            auto entry = indexCatalog->getEntryShared(indexDesc);
            auto iam = entry->accessMethod();
            _cursor = iam->getSortedDataInterface()->newCursor(_opCtx);
        }
    }
}

PlanState IndexScanStage::getNext() {
    if (!_cursor) {
        return PlanState::IS_EOF;
    }

    auto nextRecord = _cursor->next();
    if (!nextRecord) {
        return PlanState::IS_EOF;
    }

    return PlanState::ADVANCED;
}

void IndexScanStage::close() {
    _cursor.reset();
    _coll.reset();
}

std::vector<DebugPrinter::Block> IndexScanStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "ixscan");

    return ret;
}

}  // namespace sbe
}  // namespace mongo