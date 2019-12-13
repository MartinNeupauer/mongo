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
                               const std::vector<std::string>& varNames,
                               std::string_view seekKeyNameLow,
                               std::string_view seekKeyNameHi)
    : _name(name),
      _indexName(indexName),
      _recordName(recordName),
      _recordIdName(recordIdName),
      _fields(fields),
      _varNames(varNames),
      _seekKeyNameLow(seekKeyNameLow),
      _seekKeyNameHi(seekKeyNameHi) {
    invariant(_fields.size() == _varNames.size());
    invariant((_seekKeyNameLow.empty() && _seekKeyNameHi.empty()) ||
              (!_seekKeyNameLow.empty() && !_seekKeyNameHi.empty()));
}

std::unique_ptr<PlanStage> IndexScanStage::clone() {
    return std::make_unique<IndexScanStage>(_name,
                                            _indexName,
                                            _recordName,
                                            _recordIdName,
                                            _fields,
                                            _varNames,
                                            _seekKeyNameLow,
                                            _seekKeyNameHi);
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

    if (!_seekKeyNameLow.empty()) {
        _seekKeyLowAccessor = ctx.getAccessor(_seekKeyNameLow);
    }
    if (!_seekKeyNameHi.empty()) {
        _seekKeyHiAccessor = ctx.getAccessor(_seekKeyNameHi);
    }
}

value::SlotAccessor* IndexScanStage::getAccessor(CompileCtx& ctx, std::string_view field) {
    if (!_recordName.empty() && _recordName == field) {
        return _recordAccessor.get();
    }

    if (!_recordIdName.empty() && _recordIdName == field) {
        return _recordIdAccessor.get();
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

    _firstGetNext = true;

    if (auto collection = _coll->getCollection()) {
        auto indexCatalog = collection->getIndexCatalog();
        auto indexDesc = indexCatalog->findIndexByName(_opCtx, _indexName);
        if (indexDesc) {
            _weakIndexCatalogEntry = indexCatalog->getEntryShared(indexDesc);
        }

        if (auto entry = _weakIndexCatalogEntry.lock()) {
            if (!_cursor) {
                _cursor = entry->accessMethod()->getSortedDataInterface()->newCursor(_opCtx);
            }

            if (_seekKeyLowAccessor && _seekKeyHiAccessor) {
                auto [tagLow, valLow] = _seekKeyLowAccessor->getViewOfValue();
                uassert(ErrorCodes::BadValue,
                        "seek key is wrong type",
                        tagLow == value::TypeTags::ksValue);
                _seekKeyLow = value::getKeyStringView(valLow);

                auto [tagHi, valHi] = _seekKeyHiAccessor->getViewOfValue();
                uassert(ErrorCodes::BadValue,
                        "seek key is wrong type",
                        tagHi == value::TypeTags::ksValue);
                _seekKeyHi = value::getKeyStringView(valHi);
            } else {
                auto sdi = entry->accessMethod()->getSortedDataInterface();
                KeyString::Builder kb(sdi->getKeyStringVersion(),
                                      sdi->getOrdering(),
                                      KeyString::Discriminator::kExclusiveBefore);
                kb.appendDiscriminator(KeyString::Discriminator::kExclusiveBefore);
                _startPoint = kb.getValueCopy();

                _seekKeyLow = &_startPoint;
                _seekKeyHi = nullptr;
            }
        } else {
            _cursor.reset();
        }
    } else {
        _cursor.reset();
    }
}

PlanState IndexScanStage::getNext() {
    if (!_cursor) {
        return PlanState::IS_EOF;
    }


    if (_firstGetNext) {
        _firstGetNext = false;
        _nextRecord = _cursor->seekForKeyString(*_seekKeyLow);
    } else {
        _nextRecord = _cursor->nextKeyString();
    }

    if (!_nextRecord) {
        return PlanState::IS_EOF;
    }

    if (_seekKeyHi) {
        auto cmp = _nextRecord->keyString.compare(*_seekKeyHi);

        if (cmp > 0) {
            return PlanState::IS_EOF;
        }
    }

    if (_recordAccessor) {
        _recordAccessor->reset(value::TypeTags::ksValue,
                               value::bitcastFrom(&_nextRecord->keyString));
    }

    if (_recordIdAccessor) {
        _recordIdAccessor->reset(value::TypeTags::NumberInt64,
                                 value::bitcastFrom<int64_t>(_nextRecord->loc.repr()));
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