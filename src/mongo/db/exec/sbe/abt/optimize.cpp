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
#include "mongo/db/exec/sbe/abt/optimize.h"
#include "mongo/db/exec/sbe/abt/abt.h"
#include "mongo/db/exec/sbe/abt/free_vars.h"

namespace mongo {
namespace sbe {
namespace abt {
/**
 * DCE
 */
void DeadCodeElimination::optimize(ABT& root) {
    bool changed = false;
    do {
        _changed = false;
        algebra::transport(root, *this);
        changed |= _changed;
    } while (_changed);
    if (changed) {
        // update free variables
        FreeVariables fv;
        fv.compute(root);
    }
}

void DeadCodeElimination::transport(ValueBinder& op, std::vector<ABT>& binds) {
    for (auto id : op.ids()) {
        if (!op.isUsed(id) && !binds[op.index(id)].is<Blackhole>()) {
            binds[op.index(id)] = make<Blackhole>();
            _changed = true;
        }
    }
}

/**
 * Path fusion
 */
void PathFusion::optimize(ABT& root) {
    _changed = false;
    algebra::transport(root, *this);
    if (_changed) {
        // update free variables
        FreeVariables fv;
        fv.compute(root);
    }
}

bool PathFusion::fuse(ABT& lhs, ABT& rhs) {
    auto lhsGet = lhs.cast<PathGet>();
    auto rhsField = rhs.cast<PathField>();
    if (lhsGet && rhsField && lhsGet->name() == rhsField->name()) {
        std::cout << "step field\n";
        return fuse(lhsGet->t(), rhsField->t());
    }

    auto lhsTraverse = lhs.cast<PathTraverse>();
    auto rhsTraverse = rhs.cast<PathTraverse>();
    if (lhsTraverse && rhsTraverse) {
        std::cout << "step traverse\n";
        return fuse(lhsTraverse->t(), rhsTraverse->t());
    }

    if (lhs.is<PathIdentity>()) {
        std::cout << "step identity\n";
        lhs = rhs;
        return true;
    }

    if (rhs.is<PathLambda>()) {
        std::cout << "step lambda\n";
        lhs = make<PathCompose>(std::move(lhs), rhs);
        return true;
    }

    return false;
}
void PathFusion::transport(EvalPath& op, ABT& path, ABT& input) {
    auto& f = follow(input);
    if (auto eval = f.cast<EvalPath>(); eval) {
        if (fuse(op.path(), eval->path())) {
            input = eval->input();
            _changed = true;
        }
    }
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo
