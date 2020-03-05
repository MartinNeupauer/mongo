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

namespace mongo::sbe {
IndexScanStage::IndexScanStage(const NamespaceStringOrUUID& name,
                               std::string_view indexName,
                               boost::optional<value::SlotId> recordSlot,
                               boost::optional<value::SlotId> recordIdSlot,
                               const std::vector<std::string>& fields,
                               const std::vector<value::SlotId>& vars,
                               boost::optional<value::SlotId> seekKeySlotLow,
                               boost::optional<value::SlotId> seekKeySlotHi)
    : PlanStage(seekKeySlotLow ? "ixseek"_sd : "ixscan"_sd),
      _name(name),
      _indexName(indexName),
      _recordSlot(recordSlot),
      _recordIdSlot(recordIdSlot),
      _fields(fields),
      _vars(vars),
      _seekKeySlotLow(seekKeySlotLow),
      _seekKeySlotHi(seekKeySlotHi) {
    invariant(_fields.size() == _vars.size());
    // Either both boundaries are set or none is set.
    invariant((_seekKeySlotLow && _seekKeySlotHi) || (!_seekKeySlotLow && !_seekKeySlotHi));
}

std::unique_ptr<PlanStage> IndexScanStage::clone() {
    return std::make_unique<IndexScanStage>(_name,
                                            _indexName,
                                            _recordSlot,
                                            _recordIdSlot,
                                            _fields,
                                            _vars,
                                            _seekKeySlotLow,
                                            _seekKeySlotHi);
}

void IndexScanStage::prepare(CompileCtx& ctx) {
    if (*_recordSlot) {
        _recordAccessor = std::make_unique<value::ViewOfValueAccessor>();
    }

    if (*_recordIdSlot) {
        _recordIdAccessor = std::make_unique<value::ViewOfValueAccessor>();
    }

    for (size_t idx = 0; idx < _fields.size(); ++idx) {
        auto [it, inserted] =
            _fieldAccessors.emplace(_fields[idx], std::make_unique<value::ViewOfValueAccessor>());
        uassert(ErrorCodes::InternalError,
                str::stream() << "duplicate field: " << _fields[idx],
                inserted);
        auto [itRename, insertedRename] = _varAccessors.emplace(_vars[idx], it->second.get());
        uassert(ErrorCodes::InternalError,
                str::stream() << "duplicate field: " << _vars[idx],
                insertedRename);
    }

    if (_seekKeySlotLow) {
        _seekKeyLowAccessor = ctx.getAccessor(*_seekKeySlotLow);
    }
    if (_seekKeySlotHi) {
        _seekKeyHiAccessor = ctx.getAccessor(*_seekKeySlotHi);
    }
}

value::SlotAccessor* IndexScanStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    if (_recordSlot && *_recordSlot == slot) {
        return _recordAccessor.get();
    }

    if (_recordIdSlot && *_recordIdSlot == slot) {
        return _recordIdAccessor.get();
    }

    if (auto it = _varAccessors.find(slot); it != _varAccessors.end()) {
        return it->second;
    }

    return ctx.getAccessor(slot);
}

void IndexScanStage::doSaveState() {
    if (_cursor) {
        _cursor->save();
    }

    _coll.reset();
}
void IndexScanStage::doRestoreState() {
    invariant(_opCtx);
    invariant(!_coll);

    _coll.emplace(_opCtx, _name);

    if (_cursor) {
        _cursor->restore();
    }
}

void IndexScanStage::doDetachFromOperationContext() {
    if (_cursor) {
        _cursor->detachFromOperationContext();
    }
}
void IndexScanStage::doAttachFromOperationContext(OperationContext* opCtx) {
    if (_cursor) {
        _cursor->reattachToOperationContext(opCtx);
    }
}

void IndexScanStage::open(bool reOpen) {
    ScopedTimer timer(getClock(_opCtx), &_commonStats.executionTimeMillis);
    _commonStats.opens++;

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
    ScopedTimer timer(getClock(_opCtx), &_commonStats.executionTimeMillis);

    if (!_cursor) {
        return trackPlanState(PlanState::IS_EOF);
    }

    if (_firstGetNext) {
        _firstGetNext = false;
        _nextRecord = _cursor->seekForKeyString(*_seekKeyLow);
    } else {
        _nextRecord = _cursor->nextKeyString();
    }

    if (!_nextRecord) {
        return trackPlanState(PlanState::IS_EOF);
    }

    if (_seekKeyHi) {
        auto cmp = _nextRecord->keyString.compare(*_seekKeyHi);

        if (cmp > 0) {
            return trackPlanState(PlanState::IS_EOF);
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

    _specificStats.numReads++;
    return trackPlanState(PlanState::ADVANCED);
}

void IndexScanStage::close() {
    ScopedTimer timer(getClock(_opCtx), &_commonStats.executionTimeMillis);
    _commonStats.closes++;

    _cursor.reset();
    _coll.reset();
}

std::unique_ptr<PlanStageStats> IndexScanStage::getStats() const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);
    ret->specific = std::make_unique<IndexScanStats>(_specificStats);
    return ret;
}

const SpecificStats* IndexScanStage::getSpecificStats() const {
    return &_specificStats;
}

std::vector<DebugPrinter::Block> IndexScanStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;

    if (_seekKeySlotLow) {
        DebugPrinter::addKeyword(ret, "ixseek");

        DebugPrinter::addIdentifier(ret, _seekKeySlotLow.get());
        DebugPrinter::addIdentifier(ret, _seekKeySlotHi.get());
    } else {
        DebugPrinter::addKeyword(ret, "ixscan");
    }


    if (_recordSlot) {
        DebugPrinter::addIdentifier(ret, _recordSlot.get());
    }

    if (_recordIdSlot) {
        DebugPrinter::addIdentifier(ret, _recordIdSlot.get());
    }

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _fields.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _vars[idx]);
        ret.emplace_back("=");
        DebugPrinter::addIdentifier(ret, _fields[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    ret.emplace_back("@\"`");
    DebugPrinter::addIdentifier(ret, _name.toString());
    ret.emplace_back("`\"");

    ret.emplace_back("@\"`");
    DebugPrinter::addIdentifier(ret, _indexName);
    ret.emplace_back("`\"");

    return ret;
}
}  // namespace mongo::sbe
