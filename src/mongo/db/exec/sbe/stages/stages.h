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

#include "mongo/db/exec/sbe/util/debug_print.h"
#include "mongo/db/exec/sbe/values/value.h"

#include <memory>
#include <vector>

namespace mongo {
class OperationContext;
namespace sbe {

struct CompileCtx;

enum class PlanState { ADVANCED, IS_EOF };

class PlanStage {
protected:
    std::vector<std::unique_ptr<PlanStage>> _children;

    // Derived classes can optionally override these methods.
    virtual void doSaveState() {}
    virtual void doRestoreState() {}
    virtual void doDetachFromOperationContext() {}
    virtual void doAttachFromOperationContext(OperationContext* opCtx) {}

public:
    virtual ~PlanStage() {}

    // This is unspeakably ugly
    virtual std::unique_ptr<PlanStage> clone() = 0;

    virtual void prepare(CompileCtx& ctx) = 0;
    virtual value::SlotAccessor* getAccessor(CompileCtx& ctx, std::string_view field) = 0;
    virtual void open(bool reOpen) = 0;
    virtual PlanState getNext() = 0;
    virtual void close() = 0;

    // Old PlanStage state machinery
    // TODO should these be noexcept?
    void saveState() {
        for (auto& child : _children) {
            child->saveState();
        }

        doSaveState();
    }
    void restoreState() {
        for (auto& child : _children) {
            child->restoreState();
        }

        doRestoreState();
    }
    void detachFromOperationContext() {
        for (auto& child : _children) {
            child->detachFromOperationContext();
        }

        doDetachFromOperationContext();
    }
    void attachFromOperationContext(OperationContext* opCtx) {
        for (auto& child : _children) {
            child->attachFromOperationContext(opCtx);
        }

        doAttachFromOperationContext(opCtx);
    }
    virtual std::vector<DebugPrinter::Block> debugPrint() = 0;
};

template <typename T, typename... Args>
inline std::unique_ptr<PlanStage> makeS(Args&&... args) {
    return std::make_unique<T>(std::forward<Args>(args)...);
}

}  // namespace sbe
}  // namespace mongo