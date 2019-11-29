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

#include "mongo/db/exec/sbe/stages/makeobj.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/util/str.h"

namespace mongo {
namespace sbe {
MakeObjStage::MakeObjStage(std::unique_ptr<PlanStage> input,
                           std::string_view objName,
                           std::string_view rootName,
                           const std::vector<std::string>& restrictFields,
                           const std::vector<std::string>& projectFields,
                           const std::vector<std::string>& projectVarNames)
    : _objName(objName),
      _rootName(rootName),
      _restrictFields(restrictFields),
      _projectFields(projectFields),
      _projectVarNames(projectVarNames) {
    _children.emplace_back(std::move(input));
    if (_projectVarNames.size() != _projectFields.size()) {
        uasserted(ErrorCodes::InternalError, "projects and renames do not match");
    }
}
std::unique_ptr<PlanStage> MakeObjStage::clone() {
    return std::make_unique<MakeObjStage>(_children[0]->clone(),
                                          _objName,
                                          _rootName,
                                          _restrictFields,
                                          _projectFields,
                                          _projectVarNames);
}
void MakeObjStage::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);

    if (!_rootName.empty()) {
        _root = _children[0]->getAccessor(ctx, _rootName);
    }
    for (auto& p : _restrictFields) {
        auto [it, inserted] = _restrictFieldsSet.emplace(p);
        uassert(ErrorCodes::InternalError, str::stream() << "duplicate field: " << p, inserted);
    }
    for (size_t idx = 0; idx < _projectFields.size(); ++idx) {
        auto& p = _projectFields[idx];

        auto [it, inserted] = _projectFieldsSet.emplace(p);
        uassert(ErrorCodes::InternalError, str::stream() << "duplicate field: " << p, inserted);
        _projects.emplace_back(p, _children[0]->getAccessor(ctx, _projectVarNames[idx]));
    }
    _compiled = true;
}
value::SlotAccessor* MakeObjStage::getAccessor(CompileCtx& ctx, std::string_view field) {
    if (_compiled && field == _objName) {
        return &_obj;
    } else {
        return _children[0]->getAccessor(ctx, field);
    }
}
void MakeObjStage::open(bool reOpen) {
    _children[0]->open(reOpen);
}
PlanState MakeObjStage::getNext() {
    auto state = _children[0]->getNext();

    if (state == PlanState::ADVANCED) {
        auto [tag, val] = value::makeNewObject();
        auto obj = value::getObjectView(val);

        _obj.reset(tag, val);

        if (_root) {
            auto [tag, val] = _root->getViewOfValue();

            if (tag == value::TypeTags::bsonObject) {
                auto be = value::bitcastTo<const char*>(val);
                auto end = be + value::readFromMemory<uint32_t>(be);
                // skip document length
                be += 4;
                while (*be != 0) {
                    auto sv = bson::fieldNameView(be);

                    if (_restrictFieldsSet.count(sv) == 0 && _projectFieldsSet.count(sv) == 0) {
                        auto [tag, val] = bson::convertFrom(false, be, end, sv.size());
                        obj->push_back(sv, tag, val);
                    }

                    // advance
                    be = bson::advance(be, sv.size());
                }
            } else if (tag == value::TypeTags::Object) {
                auto objRoot = value::getObjectView(val);
                for (size_t idx = 0; idx < objRoot->size(); ++idx) {
                    std::string_view sv(objRoot->field(idx));

                    if (_restrictFieldsSet.count(sv) == 0 && _projectFieldsSet.count(sv) == 0) {

                        auto [tag, val] = objRoot->getAt(idx);
                        auto [copyTag, copyVal] = value::copyValue(tag, val);
                        obj->push_back(sv, copyTag, copyVal);
                    }
                }
            } else {
                // _root is not an object return it unmodified
                _obj.reset(false, tag, val);
                return state;
            }
        }
        for (auto p : _projects) {
            auto [tag, val] = p.second->getViewOfValue();
            if (tag != value::TypeTags::Nothing) {
                auto [tagCopy, valCopy] = value::copyValue(tag, val);
                obj->push_back(p.first, tagCopy, valCopy);
            }
        }
    }
    return state;
}
void MakeObjStage::close() {
    _children[0]->close();
}
std::vector<DebugPrinter::Block> MakeObjStage::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "mkobj");

    DebugPrinter::addIdentifier(ret, _objName);

    if (!_rootName.empty()) {
        DebugPrinter::addIdentifier(ret, _rootName);

        ret.emplace_back(DebugPrinter::Block("[`"));
        for (size_t idx = 0; idx < _restrictFields.size(); ++idx) {
            if (idx) {
                ret.emplace_back(DebugPrinter::Block("`,"));
            }

            DebugPrinter::addIdentifier(ret, _restrictFields[idx]);
        }
        ret.emplace_back(DebugPrinter::Block("`]"));
    }

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _projectFields.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _projectFields[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _projectVarNames.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _projectVarNames[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    DebugPrinter::addNewLine(ret);
    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());

    return ret;
}
}  // namespace sbe
}  // namespace mongo