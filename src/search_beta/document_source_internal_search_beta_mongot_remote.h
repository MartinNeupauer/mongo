/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <queue>

#include "mongo/db/pipeline/document_source.h"
#include "mongo/executor/task_executor_cursor.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {

/**
 * A class to retrieve $searchBeta results from a mongot process.
 *
 * Work slated and not handled yet:
 * - TODO Handle sharded sort merging properly (SERVER-40015)
 */
class DocumentSourceInternalSearchBetaMongotRemote final : public DocumentSource {
public:
    class LiteParsed final : public LiteParsedDocumentSource {
    public:
        static std::unique_ptr<LiteParsed> parse(const AggregationRequest& request,
                                                 const BSONElement& spec) {
            return stdx::make_unique<LiteParsed>(request.getNamespaceString());
        }

        explicit LiteParsed(NamespaceString nss) : _nss(std::move(nss)) {}

        stdx::unordered_set<NamespaceString> getInvolvedNamespaces() const final {
            return stdx::unordered_set<NamespaceString>();
        }

        PrivilegeVector requiredPrivileges(bool isMongos) const final {
            return {Privilege(ResourcePattern::forExactNamespace(_nss), ActionType::find)};
        }

        bool isInitialSource() const final {
            return true;
        }

    private:
        const NamespaceString _nss;
    };

    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    virtual ~DocumentSourceInternalSearchBetaMongotRemote() = default;

    StageConstraints constraints(Pipeline::SplitState pipeState) const override {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     HostTypeRequirement::kAnyShard,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kNotAllowed,
                                     LookupRequirement::kNotAllowed,
                                     ChangeStreamRequirement::kBlacklist);
        constraints.requiresInputDocSource = false;

        return constraints;
    }

    GetNextResult getNext() override;

    const char* getSourceName() const override;

    boost::optional<DistributedPlanLogic> distributedPlanLogic() {
        return boost::none;
    }

    Value serialize(
        boost::optional<ExplainOptions::Verbosity> explain = boost::none) const override;

private:
    static BSONObj commandObject(const BSONObj& query,
                                 const boost::intrusive_ptr<ExpressionContext>& expCtx);

    DocumentSourceInternalSearchBetaMongotRemote(
        const BSONObj& query, const boost::intrusive_ptr<ExpressionContext>& expCtx)
        : DocumentSource(expCtx), _searchBetaQuery(query.getOwned()){};

    void populateCursor();

    boost::optional<BSONObj> _getNext();

    const BSONObj _searchBetaQuery;

    boost::optional<executor::TaskExecutorCursor> _cursor;

    // Store the cursorId. We need to store it on the document source because the id on the
    // TaskExecutorCursor will be set to zero after the final getMore after the cursor is
    // exhausted.
    boost::optional<CursorId> _cursorId{boost::none};
};

}  // namespace mongo
