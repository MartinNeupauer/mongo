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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/query/sbe_stage_builder_projection.h"

#include "mongo/db/exec/sbe/stages/co_scan.h"
#include "mongo/db/exec/sbe/stages/filter.h"
#include "mongo/db/exec/sbe/stages/limit_skip.h"
#include "mongo/db/exec/sbe/stages/makeobj.h"
#include "mongo/db/exec/sbe/stages/project.h"
#include "mongo/db/exec/sbe/stages/traverse.h"
#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/db/query/sbe_stage_builder_expression.h"
#include "mongo/db/query/tree_walker.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

namespace mongo::stage_builder {
namespace {
using ExpressionType = std::unique_ptr<sbe::EExpression>;
using PlanStageType = std::unique_ptr<sbe::PlanStage>;

template <class... Ts>
struct overload : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
overload(Ts...)->overload<Ts...>;

struct ProjectionTraversalVisitorContext {
    struct NestedLevel {
        sbe::value::SlotId inputSlot;
        std::list<std::string> fields;
        std::stack<std::string> basePath;
        PlanStageType fieldPathExpressionsTraverseStage{
            sbe::makeS<sbe::LimitSkipStage>(sbe::makeS<sbe::CoScanStage>(), 1, boost::none)};
    };
    struct ProjectEval {
        sbe::value::SlotId inputSlot;
        sbe::value::SlotId outputSlot;
        std::variant<PlanStageType, ExpressionType> expr;
    };

    projection_ast::ProjectType projectType;
    sbe::value::SlotIdGenerator* slotIdGenerator;
    sbe::value::FrameIdGenerator* frameIdGenerator;
    PlanStageType inputStage;
    sbe::value::SlotId inputSlot;
    std::stack<NestedLevel> levels;
    std::stack<boost::optional<ProjectEval>> evals;

    // See the comment above the generateExpression() declaration for an explanation of the
    // 'relevantSlots' list.
    std::vector<sbe::value::SlotId> relevantSlots;

    const auto& topFrontField() const {
        invariant(!levels.empty());
        invariant(!levels.top().fields.empty());
        return levels.top().fields.front();
    }

    void popFrontField() {
        invariant(!levels.empty());
        invariant(!levels.top().fields.empty());
        levels.top().fields.pop_front();
    }

    auto& topLevel() {
        invariant(!levels.empty());
        return levels.top();
    }

    void popLevel() {
        invariant(!levels.empty());
        invariant(levels.top().fields.empty());
        levels.pop();
    }

    void pushLevel(std::list<std::string> fields) {
        levels.push({levels.empty() ? inputSlot : slotIdGenerator->generate(), std::move(fields)});
    }

    std::pair<sbe::value::SlotId, PlanStageType> done() {
        invariant(evals.size() == 1);
        auto eval = std::move(evals.top());
        invariant(eval);
        invariant(std::holds_alternative<PlanStageType>(eval->expr));
        return {eval->outputSlot,
                sbe::makeS<sbe::TraverseStage>(std::move(inputStage),
                                               std::move(std::get<PlanStageType>(eval->expr)),
                                               eval->inputSlot,
                                               eval->outputSlot,
                                               eval->outputSlot,
                                               std::vector<sbe::value::SlotId>{},
                                               nullptr,
                                               nullptr)};
    }
};

class ProjectionTraversalPreVisitor final : public projection_ast::ProjectionASTConstVisitor {
public:
    ProjectionTraversalPreVisitor(ProjectionTraversalVisitorContext* context) : _context{context} {
        invariant(_context);
    }

    void visit(const projection_ast::ProjectionPathASTNode* node) final {
        if (node->parent()) {
            _context->topLevel().basePath.push(_context->topFrontField());
            _context->popFrontField();
        }
        _context->pushLevel({node->fieldNames().begin(), node->fieldNames().end()});
    }

    void visit(const projection_ast::ProjectionPositionalASTNode* node) final {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  str::stream() << "Positional projection is not supported in SBE");
    }

    void visit(const projection_ast::ProjectionSliceASTNode* node) final {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  str::stream() << "Slice projection is not supported in SBE");
    }

    void visit(const projection_ast::ProjectionElemMatchASTNode* node) final {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  str::stream() << "ElemMatch projection is not supported in SBE");
    }

    void visit(const projection_ast::ExpressionASTNode* node) final {}

    void visit(const projection_ast::MatchExpressionASTNode* node) final {
        uasserted(ErrorCodes::InternalErrorNotSupported,
                  str::stream() << "Projection match expressions are not supported in SBE");
    }

    void visit(const projection_ast::BooleanConstantASTNode* node) final {}

private:
    ProjectionTraversalVisitorContext* _context;
};

class ProjectionTraversalPostVisitor final : public projection_ast::ProjectionASTConstVisitor {
public:
    ProjectionTraversalPostVisitor(ProjectionTraversalVisitorContext* context) : _context{context} {
        invariant(_context);
    }

    void visit(const projection_ast::BooleanConstantASTNode* node) final {
        using namespace std::literals;

        if (node->value()) {
            _context->evals.push(
                {{_context->topLevel().inputSlot,
                  _context->slotIdGenerator->generate(),
                  sbe::makeE<sbe::EFunction>(
                      "getField"sv,
                      sbe::makeEs(sbe::makeE<sbe::EVariable>(_context->topLevel().inputSlot),
                                  sbe::makeE<sbe::EConstant>(_context->topFrontField())))}});
        } else {
            _context->evals.push({});
        }
        _context->popFrontField();
    }

    void visit(const projection_ast::ExpressionASTNode* node) final {
        auto [outputSlot, expr, stage] =
            generateExpression(node->expressionRaw(),
                               std::move(_context->topLevel().fieldPathExpressionsTraverseStage),
                               _context->slotIdGenerator,
                               _context->frameIdGenerator,
                               _context->inputSlot,
                               &_context->relevantSlots);
        _context->evals.push({{_context->topLevel().inputSlot, outputSlot, std::move(expr)}});
        _context->topLevel().fieldPathExpressionsTraverseStage = std::move(stage);
        _context->popFrontField();
    }

    void visit(const projection_ast::ProjectionPathASTNode* node) final {
        using namespace std::literals;

        const auto isInclusion = _context->projectType == projection_ast::ProjectType::kInclusion;
        std::unordered_map<sbe::value::SlotId, std::unique_ptr<sbe::EExpression>> projects;
        std::vector<sbe::value::SlotId> projectSlots;
        std::vector<std::string> projectFields;
        std::vector<std::string> restrictFields;
        auto inputStage{std::move(_context->topLevel().fieldPathExpressionsTraverseStage)};

        invariant(_context->evals.size() >= node->fieldNames().size());
        for (auto it = node->fieldNames().rbegin(); it != node->fieldNames().rend(); ++it) {
            auto eval = std::move(_context->evals.top());
            _context->evals.pop();

            if (!eval) {
                restrictFields.push_back(*it);
                continue;
            }

            projectSlots.push_back(eval->outputSlot);
            projectFields.push_back(*it);

            std::visit(overload{[&](ExpressionType& expr) {
                                    projects.emplace(eval->outputSlot, std::move(expr));
                                },
                                [&](PlanStageType& stage) {
                                    invariant(!_context->topLevel().basePath.empty());

                                    inputStage = sbe::makeProjectStage(
                                        std::move(inputStage),
                                        eval->inputSlot,
                                        sbe::makeE<sbe::EFunction>(
                                            "getField"sv,
                                            sbe::makeEs(sbe::makeE<sbe::EVariable>(
                                                            _context->topLevel().inputSlot),
                                                        sbe::makeE<sbe::EConstant>(
                                                            _context->topLevel().basePath.top()))));
                                    _context->topLevel().basePath.pop();

                                    inputStage = sbe::makeS<sbe::TraverseStage>(
                                        std::move(inputStage),
                                        std::move(stage),
                                        eval->inputSlot,
                                        eval->outputSlot,
                                        eval->outputSlot,
                                        std::vector<sbe::value::SlotId>{},
                                        nullptr,
                                        nullptr);
                                }},
                       eval->expr);
        }

        std::reverse(projectFields.begin(), projectFields.end());
        std::reverse(projectSlots.begin(), projectSlots.end());

        if (!projects.empty()) {
            inputStage = sbe::makeS<sbe::ProjectStage>(std::move(inputStage), std::move(projects));
        }

        auto outputSlot = _context->slotIdGenerator->generate();
        _context->relevantSlots.push_back(outputSlot);
        _context->evals.push(
            {{_context->topLevel().inputSlot,
              outputSlot,
              isInclusion ? sbe::makeS<sbe::FilterStage<true>>(
                                sbe::makeS<sbe::MakeObjStage>(std::move(inputStage),
                                                              outputSlot,
                                                              boost::none,
                                                              std::vector<std::string>{},
                                                              projectFields,
                                                              projectSlots,
                                                              true,
                                                              false),
                                sbe::makeE<sbe::EFunction>("isObject"sv,
                                                           sbe::makeEs(sbe::makeE<sbe::EVariable>(
                                                               _context->topLevel().inputSlot))))
                          : sbe::makeS<sbe::MakeObjStage>(std::move(inputStage),
                                                          outputSlot,
                                                          _context->topLevel().inputSlot,
                                                          restrictFields,
                                                          projectFields,
                                                          projectSlots,
                                                          false,
                                                          true)}});
        _context->popLevel();
    }

    void visit(const projection_ast::ProjectionPositionalASTNode* node) final {}
    void visit(const projection_ast::ProjectionSliceASTNode* node) final {}
    void visit(const projection_ast::ProjectionElemMatchASTNode* node) final {}
    void visit(const projection_ast::MatchExpressionASTNode* node) final {}

private:
    ProjectionTraversalVisitorContext* _context;
};

class ProjectionTraversalWalker final {
public:
    ProjectionTraversalWalker(projection_ast::ProjectionASTConstVisitor* preVisitor,
                              projection_ast::ProjectionASTConstVisitor* postVisitor)
        : _preVisitor{preVisitor}, _postVisitor{postVisitor} {}

    void preVisit(const projection_ast::ASTNode* node) {
        node->acceptVisitor(_preVisitor);
    }

    void postVisit(const projection_ast::ASTNode* node) {
        node->acceptVisitor(_postVisitor);
    }

    void inVisit(long count, const projection_ast::ASTNode* node) {}

private:
    projection_ast::ProjectionASTConstVisitor* _preVisitor;
    projection_ast::ProjectionASTConstVisitor* _postVisitor;
};
}  // namespace

std::pair<sbe::value::SlotId, PlanStageType> generateProjection(
    const projection_ast::Projection* projection,
    PlanStageType stage,
    sbe::value::SlotIdGenerator* slotIdGenerator,
    sbe::value::FrameIdGenerator* frameIdGenerator,
    sbe::value::SlotId inputVar) {
    ProjectionTraversalVisitorContext context{
        projection->type(), slotIdGenerator, frameIdGenerator, std::move(stage), inputVar};
    context.relevantSlots.push_back(inputVar);
    ProjectionTraversalPreVisitor preVisitor{&context};
    ProjectionTraversalPostVisitor postVisitor{&context};
    ProjectionTraversalWalker walker{&preVisitor, &postVisitor};
    tree_walker::walk<true, projection_ast::ASTNode>(projection->root(), &walker);
    return context.done();
}
}  // namespace mongo::stage_builder
