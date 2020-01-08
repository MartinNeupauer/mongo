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

#include "mongo/db/exec/sbe/stages/scan.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/util/str.h"

namespace mongo {
namespace sbe {
ScanStage::ScanStage(const NamespaceStringOrUUID& name,
                     boost::optional<value::SlotId> recordSlot,
                     boost::optional<value::SlotId> recordIdSlot,
                     const std::vector<std::string>& fields,
                     const std::vector<value::SlotId>& vars,
                     boost::optional<value::SlotId> seekKeySlot)
    : _name(name),
      _recordSlot(recordSlot),
      _recordIdSlot(recordIdSlot),
      _fields(fields),
      _vars(vars),
      _seekKeySlot(seekKeySlot) {
    invariant(_fields.size() == _vars.size());
}

std::unique_ptr<PlanStage> ScanStage::clone() {
    return std::make_unique<ScanStage>(
        _name, _recordSlot, _recordIdSlot, _fields, _vars, _seekKeySlot);
}

void ScanStage::prepare(CompileCtx& ctx) {
    if (_recordSlot) {
        _recordAccessor = std::make_unique<value::ViewOfValueAccessor>();
    }

    if (_recordIdSlot) {
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

    if (_seekKeySlot) {
        _seekKeyAccessor = ctx.getAccessor(*_seekKeySlot);
    }
}

value::SlotAccessor* ScanStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
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

void ScanStage::doSaveState() {
    if (_cursor) {
        _cursor->save();
    }

    invariant(_coll);
    _coll.reset();
}
void ScanStage::doRestoreState() {
    invariant(_opCtx);
    invariant(!_coll);

    _coll.emplace(_opCtx, _name);

    if (_cursor) {
        const bool couldRestore = _cursor->restore();
        uassert(ErrorCodes::CappedPositionLost,
                str::stream()
                    << "CollectionScan died due to position in capped collection being deleted. ",
                couldRestore);
    }
}

void ScanStage::doDetachFromOperationContext() {
    invariant(_opCtx);
    if (_cursor) {
        _cursor->detachFromOperationContext();
    }
    _opCtx = nullptr;
}
void ScanStage::doAttachFromOperationContext(OperationContext* opCtx) {
    invariant(opCtx);
    invariant(!_opCtx);
    if (_cursor) {
        _cursor->reattachToOperationContext(opCtx);
    }
    _opCtx = opCtx;
}

void ScanStage::open(bool reOpen) {
    invariant(_opCtx);
    if (!reOpen) {
        invariant(!_cursor);
        invariant(!_coll);
        _coll.emplace(_opCtx, _name);
    } else {
        invariant(_cursor);
        invariant(_coll);
    }

    if (auto collection = _coll->getCollection()) {
        if (_seekKeyAccessor) {
            auto [tag, val] = _seekKeyAccessor->getViewOfValue();
            uassert(ErrorCodes::BadValue,
                    "seek key is wrong type",
                    tag == value::TypeTags::NumberInt64);

            _key = RecordId{value::bitcastTo<int64_t>(val)};
        }

        if (!_cursor || !_seekKeyAccessor) {
            _cursor = collection->getCursor(_opCtx);
        }
    } else {
        _cursor.reset();
    }

    _firstGetNext = true;
}

PlanState ScanStage::getNext() {
    if (!_cursor) {
        return PlanState::IS_EOF;
    }

    auto nextRecord =
        (_firstGetNext && _seekKeyAccessor) ? _cursor->seekExact(_key) : _cursor->next();
    _firstGetNext = false;

    if (!nextRecord) {
        return PlanState::IS_EOF;
    }

    if (_seekKeyAccessor && nextRecord->id != _key) {
        return PlanState::IS_EOF;
    }

    if (_recordAccessor) {
        _recordAccessor->reset(value::TypeTags::bsonObject,
                               value::bitcastFrom<const char*>(nextRecord->data.data()));
    }

    if (_recordIdAccessor) {
        _recordIdAccessor->reset(value::TypeTags::NumberInt64,
                                 value::bitcastFrom<int64_t>(nextRecord->id.repr()));
    }


    if (!_fieldAccessors.empty()) {
        auto fieldsToMatch = _fieldAccessors.size();
        auto rawBson = nextRecord->data.data();
        auto be = rawBson + 4;
        auto end = rawBson + value::readFromMemory<uint32_t>(rawBson);
        for (auto& [name, accessor] : _fieldAccessors) {
            accessor->reset();
        }
        while (*be != 0) {
            auto sv = bson::fieldNameView(be);
            if (auto it = _fieldAccessors.find(sv); it != _fieldAccessors.end()) {
                // found the field so convert it to Value
                auto [tag, val] = bson::convertFrom(true, be, end, sv.size());

                it->second->reset(tag, val);

                if ((--fieldsToMatch) == 0) {
                    // not need to scan any further so bail out early
                    break;
                }
            }

            // advance
            be = bson::advance(be, sv.size());
        }
    }

    return PlanState::ADVANCED;
}

void ScanStage::close() {
    _cursor.reset();
    _coll.reset();
}

std::vector<DebugPrinter::Block> ScanStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "scan");
    return ret;
}

ParallelScanStage::ParallelScanStage(const NamespaceStringOrUUID& name,
                                     boost::optional<value::SlotId> recordSlot,
                                     boost::optional<value::SlotId> recordIdSlot,
                                     const std::vector<std::string>& fields,
                                     const std::vector<value::SlotId>& vars)
    : _name(name),
      _recordSlot(recordSlot),
      _recordIdSlot(recordIdSlot),
      _fields(fields),
      _vars(vars) {
    invariant(_fields.size() == _vars.size());

    _state = std::make_shared<ParallelState>();
}

ParallelScanStage::ParallelScanStage(const std::shared_ptr<ParallelState>& state,
                                     const NamespaceStringOrUUID& name,
                                     boost::optional<value::SlotId> recordSlot,
                                     boost::optional<value::SlotId> recordIdSlot,
                                     const std::vector<std::string>& fields,
                                     const std::vector<value::SlotId>& vars)
    : _name(name),
      _recordSlot(recordSlot),
      _recordIdSlot(recordIdSlot),
      _fields(fields),
      _vars(vars),
      _state(state) {
    invariant(_fields.size() == _vars.size());
}
std::unique_ptr<PlanStage> ParallelScanStage::clone() {
    return std::make_unique<ParallelScanStage>(
        _state, _name, _recordSlot, _recordIdSlot, _fields, _vars);
}

void ParallelScanStage::prepare(CompileCtx& ctx) {
    if (_recordSlot) {
        _recordAccessor = std::make_unique<value::ViewOfValueAccessor>();
    }

    if (_recordIdSlot) {
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
}

value::SlotAccessor* ParallelScanStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
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

void ParallelScanStage::doSaveState() {
    if (_cursor) {
        _cursor->save();
    }
}
void ParallelScanStage::doRestoreState() {
    if (_cursor) {
        const bool couldRestore = _cursor->restore();
        uassert(ErrorCodes::CappedPositionLost,
                str::stream()
                    << "CollectionScan died due to position in capped collection being deleted. ",
                couldRestore);
    }
}

void ParallelScanStage::doDetachFromOperationContext() {
    invariant(_opCtx);
    if (_cursor) {
        _cursor->detachFromOperationContext();
    }
    _opCtx = nullptr;
}
void ParallelScanStage::doAttachFromOperationContext(OperationContext* opCtx) {
    invariant(opCtx);
    invariant(!_opCtx);
    if (_cursor) {
        _cursor->reattachToOperationContext(opCtx);
    }
    _opCtx = opCtx;
}

void ParallelScanStage::open(bool reOpen) {
    invariant(_opCtx);
    invariant(!reOpen, "parallel scan is not restartable");

    invariant(!_cursor);
    invariant(!_coll);
    _coll.emplace(_opCtx, _name);
    auto collection = _coll->getCollection();

    if (collection) {
        {
            std::unique_lock lock(_state->mutex);
            if (_state->ranges.empty()) {
                auto ranges = collection->getRecordStore()->numRecords(_opCtx) / 10240;
                if (ranges < 2) {
                    _state->ranges.emplace_back(Range{RecordId{}, RecordId{}});
                } else {
                    if (ranges > 1024) {
                        ranges = 1024;
                    }
                    auto randomCursor = collection->getRecordStore()->getRandomCursor(_opCtx);
                    invariant(randomCursor);
                    std::set<RecordId> rids;
                    while (ranges--) {
                        auto nextRecord = randomCursor->next();
                        if (nextRecord) {
                            rids.emplace(nextRecord->id);
                        }
                    }
                    RecordId lastid{};
                    for (auto id : rids) {
                        _state->ranges.emplace_back(Range{lastid, id});
                        lastid = id;
                    }
                    _state->ranges.emplace_back(Range{lastid, RecordId{}});
                }
            }
        }

        _cursor = collection->getCursor(_opCtx);
    }
}

boost::optional<Record> ParallelScanStage::nextRange() {
    invariant(_cursor);
    _currentRange = _state->currentRange++;
    if (_currentRange < _state->ranges.size()) {
        _range = _state->ranges[_currentRange];

        return _range.begin.isNull() ? _cursor->next() : _cursor->seekExact(_range.begin);
    } else {
        return boost::none;
    }
}

PlanState ParallelScanStage::getNext() {
    if (!_cursor) {
        return PlanState::IS_EOF;
    }

    boost::optional<Record> nextRecord;

    do {
        nextRecord = needsRange() ? nextRange() : _cursor->next();
        if (!nextRecord) {
            return PlanState::IS_EOF;
        }

        if (!_range.end.isNull() && nextRecord->id == _range.end) {
            setNeedsRange();
            nextRecord = boost::none;
        }
    } while (!nextRecord);

    if (_recordAccessor) {
        _recordAccessor->reset(value::TypeTags::bsonObject,
                               value::bitcastFrom<const char*>(nextRecord->data.data()));
    }

    if (_recordIdAccessor) {
        _recordIdAccessor->reset(value::TypeTags::NumberInt64,
                                 value::bitcastFrom<int64_t>(nextRecord->id.repr()));
    }


    if (!_fieldAccessors.empty()) {
        auto fieldsToMatch = _fieldAccessors.size();
        auto rawBson = nextRecord->data.data();
        auto be = rawBson + 4;
        auto end = rawBson + value::readFromMemory<uint32_t>(rawBson);
        for (auto& [name, accessor] : _fieldAccessors) {
            accessor->reset();
        }
        while (*be != 0) {
            auto sv = bson::fieldNameView(be);
            if (auto it = _fieldAccessors.find(sv); it != _fieldAccessors.end()) {
                // found the field so convert it to Value
                auto [tag, val] = bson::convertFrom(true, be, end, sv.size());

                it->second->reset(tag, val);

                if ((--fieldsToMatch) == 0) {
                    // not need to scan any further so bail out early
                    break;
                }
            }

            // advance
            be = bson::advance(be, sv.size());
        }
    }

    return PlanState::ADVANCED;
}

void ParallelScanStage::close() {
    _cursor.reset();
    _coll.reset();
}

std::vector<DebugPrinter::Block> ParallelScanStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "pscan");
    return ret;
}
}  // namespace sbe
}  // namespace mongo
