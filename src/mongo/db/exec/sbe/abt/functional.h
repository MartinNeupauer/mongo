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

#include "mongo/db/exec/sbe/abt/base.h"

namespace mongo {
namespace sbe {
namespace abt {
class FDep final : public OperatorDynamic<FDep, 0>, public ValueSyntaxSort {
    using Base = OperatorDynamic<FDep, 0>;

    Type _type;

public:
    const Type& type() const override {
        return _type;
    }

    FDep(Type typeIn, std::vector<ABT> deps);
};

class EvalPath final : public Operator<EvalPath, 2>, public ValueSyntaxSort {
    using Base = Operator<EvalPath, 2>;

    Type _type;

public:
    const Type& type() const override {
        return _type;
    }

    const auto& path() const {
        return get<0>();
    }
    const auto& input() const {
        return get<1>();
    }
    EvalPath(ABT pathIn, ABT inputIn);
};

class FunctionCall final : public OperatorDynamic<FunctionCall, 0>, public ValueSyntaxSort {
    using Base = OperatorDynamic<FunctionCall, 0>;

    Type _type;
    std::string _name;

public:
    const Type& type() const override {
        return _type;
    }

    FunctionCall(std::string nameIn, std::vector<ABT> argsIn);
};

class If final : public Operator<If, 3>, public ValueSyntaxSort {
    using Base = Operator<If, 3>;

public:
    const Type& type() const override {
        return get<1>().cast<ValueSyntaxSort>()->type();
    }

    If(ABT condIn, ABT thenIn, ABT elseIn);
};

class BinaryOp final : public Operator<BinaryOp, 2>, public ValueSyntaxSort {
    using Base = Operator<BinaryOp, 2>;

public:
    // TODO unify with execution
    enum Op { logicAnd, logicOr };

private:
    Type _type;
    Op _op;

public:
    const Type& type() const override {
        return _type;
    }

    BinaryOp(Op opIn, ABT lhs, ABT rhs);
};

class UnaryOp final : public Operator<UnaryOp, 1>, public ValueSyntaxSort {
    using Base = Operator<UnaryOp, 1>;

public:
    // TODO unify with execution
    enum Op { logicNot };

private:
    Type _type;
    Op _op;

public:
    const Type& type() const override {
        return _type;
    }

    UnaryOp(Op opIn, ABT arg);
};

class LocalBind : public Operator<LocalBind, 2>, public ValueSyntaxSort {
    using Base = Operator<LocalBind, 2>;

public:
    const Type& type() const override {
        return get<1>().cast<ValueSyntaxSort>()->type();
    }

    LocalBind(ABT bindIn, ABT inIn);
};

/**
 * We dont have lambda application yet so this is used only by paths.
 */
class LambdaAbstraction : public Operator<LambdaAbstraction, 2>, public ValueSyntaxSort {
    using Base = Operator<LambdaAbstraction, 2>;
    Type _type;

public:
    const Type& type() const override {
        return _type;
    }

    LambdaAbstraction(ABT paramIn, ABT bodyIn);
};

/**
 * Represents a function (lambda) parameter
 */
class BoundParameter : public Operator<BoundParameter, 0>, public ValueSyntaxSort {
    Type _type;

public:
    const Type& type() const override {
        return _type;
    }

    BoundParameter(Type typeIn) : _type(std::move(typeIn)) {}
};

/**
 * The optimization fence.
 */
class OptFence : public Operator<OptFence, 1>, public ValueSyntaxSort {
    using Base = Operator<OptFence, 1>;

public:
    const Type& type() const override {
        return get<0>().cast<ValueSyntaxSort>()->type();
    }

    OptFence(ABT input);
};

/**
 * Simple single argument lambda
 */
template <typename T>
inline auto lam(VarId p, T&& body) {
    return make<LambdaAbstraction>(makeBinder(p, make<BoundParameter>(kVariantType)),
                                   std::forward<T>(body));
}

template <typename... Args>
inline auto fdep(Type type, Args&&... args) {
    return make<FDep>(std::move(type), makeSeq(std::forward<Args>(args)...));
}

template <typename... Args>
inline auto fun(std::string name, Args&&... args) {
    return make<FunctionCall>(std::move(name), makeSeq(std::forward<Args>(args)...));
}
template <typename T>
inline auto _if(T&& cond, T&& t, T&& e) {
    return make<If>(std::forward<T>(cond), std::forward<T>(t), std::forward<T>(e));
}

template <typename T>
inline auto op(BinaryOp::Op op, T&& l, T&& r) {
    return make<BinaryOp>(op, std::forward<T>(l), std::forward<T>(r));
}

template <typename T>
inline auto op(UnaryOp::Op op, T&& l) {
    return make<UnaryOp>(op, std::forward<T>(l));
}

template <typename T>
inline auto fence(T&& input) {
    return make<OptFence>(std::forward<T>(input));
}
}  // namespace abt
}  // namespace sbe
}  // namespace mongo