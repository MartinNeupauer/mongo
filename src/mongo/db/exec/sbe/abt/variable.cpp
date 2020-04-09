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

#include "mongo/db/exec/sbe/abt/abt.h"
#include "mongo/db/exec/sbe/abt/exe_generator.h"
#include "mongo/db/exec/sbe/abt/free_vars.h"
#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace sbe {
namespace abt {
Variable::Variable(const Variable& other) : _id(other._id) {
    // TODO - fix binder copying
    // rebind(other._binding);
    rebind(nullptr);
}
Variable::~Variable() {
    rebind(nullptr);
}
void Variable::rebind(ValueBinder* b) {
    if (_binding) {
        _binding->removeReference(this);
    }
    if (b) {
        b->addReference(this);
    }

    _binding = b;
}

const ABT& Variable::followVar() const {
    invariant(_binding);
    return follow(_binding->binds()[_binding->index(_id)]);
}

ABT& Variable::followVar() {
    invariant(_binding);
    return follow(_binding->binds()[_binding->index(_id)]);
}
/**
 * Free variables
 */
ABT* FreeVariables::transport(ABT& e, Variable& op) {
    // Add this variable to the list of free variables.
    addFreeVar(&e);

    return &e;
}

/**
 * ExeGenerator
 */
ExeGenerator::GenResult ExeGenerator::walk(const Variable& op) {
    invariant(op.binding());
    auto it = _slots.find(op.binding());
    invariant(it != _slots.end());

    auto& info = it->second[op.binding()->index(op.id())];
    invariant(info.slot);

    GenResult result;
    if (info.frame) {
        result.expr = makeE<EVariable>(*info.frame, *info.slot);
    } else {
        result.expr = makeE<EVariable>(*info.slot);
        result.slot = *info.slot;
    }
    return result;
}
ExeGenerator::GenResult ExeGenerator::walk(const Blackhole& op) {
    uasserted(ErrorCodes::InternalError, "unexpected blackhole");
    MONGO_UNREACHABLE;
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
