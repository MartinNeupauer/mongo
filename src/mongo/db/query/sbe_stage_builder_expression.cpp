/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/query/sbe_stage_builder_expression.h"

#include "mongo/db/exec/sbe/stages/co_scan.h"
#include "mongo/db/exec/sbe/stages/limit_skip.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/db/exec/sbe/stages/traverse.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/pipeline/accumulator.h"
#include "mongo/db/pipeline/expression_visitor.h"
#include "mongo/db/pipeline/expression_walker.h"
#include "mongo/db/query/projection_parser.h"
#include "mongo/util/log.h"
#include "mongo/util/str.h"

namespace mongo::stage_builder {
namespace {
std::pair<sbe::value::TypeTags, sbe::value::Value> convertFrom(Value val) {
    // TODO: need to do something smarter than that
    BSONObjBuilder bob;
    val.addToBsonObj(&bob, ""_sd);
    auto obj = bob.done();
    auto be = obj.objdata();
    auto end = be + sbe::value::readFromMemory<uint32_t>(be);
    return sbe::bson::convertFrom(false, be + 4, end, 0);
}

struct ExpressionVisitorContext {
    std::unique_ptr<sbe::PlanStage> traverseStage;
    sbe::value::SlotIdGenerator* slotIdGenerator{nullptr};
    sbe::value::SlotId rootSlot;
    std::stack<std::unique_ptr<sbe::EExpression>> exprs;

    void ensureArity(size_t arity) {
        invariant(exprs.size() >= arity);
    }

    std::unique_ptr<sbe::EExpression> popExpr() {
        auto expr = std::move(exprs.top());
        exprs.pop();
        return expr;
    }

    void pushExpr(std::unique_ptr<sbe::EExpression> expr) {
        exprs.push(std::move(expr));
    }

    void pushExpr(std::unique_ptr<sbe::EExpression> expr, std::unique_ptr<sbe::PlanStage> stage) {
        exprs.push(std::move(expr));
        traverseStage = std::move(stage);
    }

    std::tuple<sbe::value::SlotId,
               std::unique_ptr<sbe::EExpression>,
               std::unique_ptr<sbe::PlanStage>>
    done() {
        invariant(exprs.size() == 1);
        return {slotIdGenerator->generate(), popExpr(), std::move(traverseStage)};
    }
};

std::pair<sbe::value::SlotId, std::unique_ptr<sbe::PlanStage>> generateTraverseHelper(
    std::unique_ptr<sbe::PlanStage> inputStage,
    sbe::value::SlotId inputSlot,
    const FieldPath& fp,
    size_t level,
    sbe::value::SlotIdGenerator* slotIdGenerator) {
    using namespace std::literals;

    invariant(level < fp.getPathLength());

    // The field we will be traversing at the current nested level.
    auto fieldSlot{slotIdGenerator->generate()};
    // The result coming from the 'in' branch of the traverse plan stage.
    auto outputSlot{slotIdGenerator->generate()};

    // Generate the projection stage to read a sub-field at the current nested level and bind it
    // to 'fieldSlot'.
    inputStage = sbe::makeProjectStage(
        std::move(inputStage),
        fieldSlot,
        sbe::makeE<sbe::EFunction>(
            "getField"sv,
            sbe::makeEs(sbe::makeE<sbe::EVariable>(inputSlot), sbe::makeE<sbe::EConstant>([&]() {
                            auto fieldName = fp.getFieldName(level);
                            return std::string_view{fieldName.rawData(), fieldName.size()};
                        }()))));

    std::unique_ptr<sbe::PlanStage> innerBranch;
    if (level == fp.getPathLength() - 1) {
        innerBranch = sbe::makeProjectStage(
            sbe::makeS<sbe::LimitSkipStage>(sbe::makeS<sbe::CoScanStage>(), 1, boost::none),
            outputSlot,
            sbe::makeE<sbe::EVariable>(fieldSlot));
    } else {
        // Generate nested traversal.
        auto [slot, stage] = generateTraverseHelper(
            sbe::makeS<sbe::LimitSkipStage>(sbe::makeS<sbe::CoScanStage>(), 1, boost::none),
            fieldSlot,
            fp,
            level + 1,
            slotIdGenerator);
        innerBranch =
            sbe::makeProjectStage(std::move(stage), outputSlot, sbe::makeE<sbe::EVariable>(slot));
    }

    // The final traverse stage for the current nested level.
    return {outputSlot,
            sbe::makeS<sbe::TraverseStage>(std::move(inputStage),
                                           std::move(innerBranch),
                                           fieldSlot,
                                           outputSlot,
                                           outputSlot,
                                           nullptr,
                                           nullptr,
                                           1)};
}

/**
 * For the given MatchExpression 'expr', generates a path traversal SBE plan stage sub-tree
 * implementing the comparison expression.
 */
std::pair<sbe::value::SlotId, std::unique_ptr<sbe::PlanStage>> generateTraverse(
    std::unique_ptr<sbe::PlanStage> inputStage,
    sbe::value::SlotId inputSlot,
    const FieldPath& fp,
    sbe::value::SlotIdGenerator* slotIdGenerator) {
    return generateTraverseHelper(std::move(inputStage), inputSlot, fp, 0, slotIdGenerator);
}

class ExpressionPostVisitor final : public ExpressionVisitor {
public:
    ExpressionPostVisitor(ExpressionVisitorContext* context) : _context{context} {}

    void visit(ExpressionConstant* expr) final {
        auto [tag, val] = convertFrom(expr->getValue());
        _context->pushExpr(sbe::makeE<sbe::EConstant>(tag, val));
    }

    void visit(ExpressionAbs* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionAdd* expr) final {
        _context->ensureArity(2);
        auto rhs = _context->popExpr();
        auto lhs = _context->popExpr();
        _context->pushExpr(
            sbe::makeE<sbe::EPrimBinary>(sbe::EPrimBinary::add, std::move(lhs), std::move(rhs)));
    }
    void visit(ExpressionAllElementsTrue* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionAnd* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionAnyElementTrue* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionArray* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionArrayElemAt* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionFirst* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionLast* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionObjectToArray* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionArrayToObject* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionBsonSize* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionCeil* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionCoerceToBool* expr) final {
        unsupportedExpression("$coerceToBool");
    }
    void visit(ExpressionCompare* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionConcat* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionConcatArrays* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionCond* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionDateFromString* expr) final {
        unsupportedExpression("$dateFromString");
    }
    void visit(ExpressionDateFromParts* expr) final {
        unsupportedExpression("$dateFromString");
    }
    void visit(ExpressionDateToParts* expr) final {
        unsupportedExpression("$dateFromString");
    }
    void visit(ExpressionDateToString* expr) final {
        unsupportedExpression("$dateFromString");
    }
    void visit(ExpressionDivide* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionExp* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionFieldPath* expr) final {
        auto [outputSlot, stage] = generateTraverse(std::move(_context->traverseStage),
                                                    _context->rootSlot,
                                                    expr->getFieldPathWithoutCurrentPrefix(),
                                                    _context->slotIdGenerator);
        _context->pushExpr(sbe::makeE<sbe::EVariable>(outputSlot), std::move(stage));
    }
    void visit(ExpressionFilter* expr) final {
        unsupportedExpression("$filter");
    }
    void visit(ExpressionFloor* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionIfNull* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionIn* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionIndexOfArray* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionIndexOfBytes* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionIndexOfCP* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionIsNumber* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionLet* expr) final {
        unsupportedExpression("$let");
    }
    void visit(ExpressionLn* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionLog* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionLog10* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionMap* expr) final {
        unsupportedExpression("$map");
    }
    void visit(ExpressionMeta* expr) final {
        unsupportedExpression("$meta");
    }
    void visit(ExpressionMod* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionMultiply* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionNot* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionObject* expr) final {
        unsupportedExpression("$object");
    }
    void visit(ExpressionOr* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionPow* expr) final {
        unsupportedExpression("$pow");
    }
    void visit(ExpressionRange* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionReduce* expr) final {
        unsupportedExpression("$reduce");
    }
    void visit(ExpressionReplaceOne* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionReplaceAll* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSetDifference* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSetEquals* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSetIntersection* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSetIsSubset* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSetUnion* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSize* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionReverseArray* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSlice* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionIsArray* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionRound* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSplit* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSqrt* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionStrcasecmp* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSubstrBytes* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSubstrCP* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionStrLenBytes* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionBinarySize* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionStrLenCP* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSubtract* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionSwitch* expr) final {
        unsupportedExpression("$switch");
    }
    void visit(ExpressionToLower* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionToUpper* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionTrim* expr) final {
        unsupportedExpression("$trim");
    }
    void visit(ExpressionTrunc* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionType* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionZip* expr) final {
        unsupportedExpression("$zip");
    }
    void visit(ExpressionConvert* expr) final {
        unsupportedExpression("$convert");
    }
    void visit(ExpressionRegexFind* expr) final {
        unsupportedExpression("$regexFind");
    }
    void visit(ExpressionRegexFindAll* expr) final {
        unsupportedExpression("$regexFind");
    }
    void visit(ExpressionRegexMatch* expr) final {
        unsupportedExpression("$regexFind");
    }
    void visit(ExpressionCosine* expr) final {
        unsupportedExpression("$cos");
    }
    void visit(ExpressionSine* expr) final {
        unsupportedExpression("$sin");
    }
    void visit(ExpressionTangent* expr) final {
        unsupportedExpression("$tan");
    }
    void visit(ExpressionArcCosine* expr) final {
        unsupportedExpression("$acos");
    }
    void visit(ExpressionArcSine* expr) final {
        unsupportedExpression("$asin");
    }
    void visit(ExpressionArcTangent* expr) final {
        unsupportedExpression("$atan");
    }
    void visit(ExpressionArcTangent2* expr) final {
        unsupportedExpression("$atan2");
    }
    void visit(ExpressionHyperbolicArcTangent* expr) final {
        unsupportedExpression("$atanh");
    }
    void visit(ExpressionHyperbolicArcCosine* expr) final {
        unsupportedExpression("$acosh");
    }
    void visit(ExpressionHyperbolicArcSine* expr) final {
        unsupportedExpression("$asinh");
    }
    void visit(ExpressionHyperbolicTangent* expr) final {
        unsupportedExpression("$tanh");
    }
    void visit(ExpressionHyperbolicCosine* expr) final {
        unsupportedExpression("$cosh");
    }
    void visit(ExpressionHyperbolicSine* expr) final {
        unsupportedExpression("$sinh");
    }
    void visit(ExpressionDegreesToRadians* expr) final {
        unsupportedExpression("$degreesToRadians");
    }
    void visit(ExpressionRadiansToDegrees* expr) final {
        unsupportedExpression("$radiansToDegrees");
    }
    void visit(ExpressionDayOfMonth* expr) final {
        unsupportedExpression("$dayOfMonth");
    }
    void visit(ExpressionDayOfWeek* expr) final {
        unsupportedExpression("$dayOfWeek");
    }
    void visit(ExpressionDayOfYear* expr) final {
        unsupportedExpression("$dayOfYear");
    }
    void visit(ExpressionHour* expr) final {
        unsupportedExpression("$hour");
    }
    void visit(ExpressionMillisecond* expr) final {
        unsupportedExpression("$millisecond");
    }
    void visit(ExpressionMinute* expr) final {
        unsupportedExpression("$minute");
    }
    void visit(ExpressionMonth* expr) final {
        unsupportedExpression("$month");
    }
    void visit(ExpressionSecond* expr) final {
        unsupportedExpression("$second");
    }
    void visit(ExpressionWeek* expr) final {
        unsupportedExpression("$week");
    }
    void visit(ExpressionIsoWeekYear* expr) final {
        unsupportedExpression("$isoWeekYear");
    }
    void visit(ExpressionIsoDayOfWeek* expr) final {
        unsupportedExpression("$isoDayOfWeek");
    }
    void visit(ExpressionIsoWeek* expr) final {
        unsupportedExpression("$isoWeek");
    }
    void visit(ExpressionYear* expr) final {
        unsupportedExpression("$year");
    }
    void visit(ExpressionFromAccumulator<AccumulatorAvg>* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionFromAccumulator<AccumulatorMax>* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionFromAccumulator<AccumulatorMin>* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevPop>* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionFromAccumulator<AccumulatorStdDevSamp>* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionFromAccumulator<AccumulatorSum>* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionFromAccumulator<AccumulatorMergeObjects>* expr) final {
        unsupportedExpression(expr->getOpName());
    }
    void visit(ExpressionTests::Testable* expr) final {
        unsupportedExpression("$test");
    }
    void visit(ExpressionInternalJsEmit* expr) final {
        unsupportedExpression("$internalJsEmit");
    }
    void visit(ExpressionInternalFindSlice* expr) final {
        unsupportedExpression("$internalFindSlice");
    }
    void visit(ExpressionInternalFindPositional* expr) final {
        unsupportedExpression("$internalFindPositional");
    }
    void visit(ExpressionInternalFindElemMatch* expr) final {
        unsupportedExpression("$internalFindElemMatch");
    }
    void visit(ExpressionFunction* expr) final {
        unsupportedExpression("$function");
    }

private:
    void unsupportedExpression(const char* op) const {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  str::stream() << "Expression is not supported in SBE: " << op);
    }

    ExpressionVisitorContext* _context;
};

class ExpressionWalker final {
public:
    ExpressionWalker(ExpressionVisitor* postVisitor) : _postVisitor{postVisitor} {}

    void postVisit(Expression* expr) {
        expr->acceptVisitor(_postVisitor);
    }

    void preVisit(Expression* expr) {}
    void inVisit(long long count, Expression* expr) {}

private:
    ExpressionVisitor* _postVisitor;
};
}  // namespace

std::tuple<sbe::value::SlotId, std::unique_ptr<sbe::EExpression>, std::unique_ptr<sbe::PlanStage>>
generateExpression(Expression* expr,
                   std::unique_ptr<sbe::PlanStage> stage,
                   sbe::value::SlotIdGenerator* slotIdGenerator,
                   sbe::value::SlotId rootSlot) {
    ExpressionVisitorContext context{std::move(stage), slotIdGenerator, rootSlot};
    ExpressionPostVisitor postVisitor{&context};
    ExpressionWalker walker{&postVisitor};
    expression_walker::walk(&walker, expr);
    return context.done();
}
}  // namespace mongo::stage_builder
