/**
 *    Copyright (C) 2018 MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <deque>
#include <vector>

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_sources_gen.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"

namespace mongo {

class Exchange : public RefCountable {
    static constexpr size_t kInvalidThreadId{std::numeric_limits<size_t>::max()};

    static std::vector<std::string> extractBoundaries(
        const boost::optional<std::vector<BSONObj>>& obj);

public:
    Exchange(const ExchangeSpec& spec);
    DocumentSource::GetNextResult getNext(size_t consumerId);

    size_t consumers() const {
        return _consumers.size();
    }

    void setSource(DocumentSource* source) {
        pSource = source;
    }

    auto& spec() const {
        return _spec;
    }

private:
    size_t loadNextBatch();

    size_t getTargetConsumer(const Document& input);

    class ExchangeBuffer {
    public:
        bool appendDocument(DocumentSource::GetNextResult input, size_t limit);
        DocumentSource::GetNextResult getNext();
        bool isEmpty() const {
            return _buffer.empty();
        }

    private:
        size_t _bytesInBuffer{0};
        std::deque<DocumentSource::GetNextResult> _buffer;
    };

    // Keep a copy of the spec for serialization purposes.
    const ExchangeSpec _spec;

    // A pattern for extracting a key from a document used by range and hash policies.
    const BSONObj _keyPattern;

    // Range boundaries.
    const std::vector<std::string> _boundaries;

    // A policy that tells how to distribute input documents to consumers.
    const ExchangePolicyEnum _policy;

    // If set to true then a producer sends special 'high watermark' documents to consumers in order
    // to prevent deadlocks.
    const bool _orderPreserving;

    // A maximum size of buffer per consumer.
    const size_t _maxBufferSize;

    // An input to the exchange operator
    DocumentSource* pSource;

    // Synchronization.
    stdx::mutex _mutex;
    stdx::condition_variable _condition;

    // A thread that is currently loading the exchange buffers.
    size_t _loadingThreadId{kInvalidThreadId};

    size_t _roundRobinCounter{0};

    std::vector<std::unique_ptr<ExchangeBuffer>> _consumers;
};

class DocumentSourceExchange final : public DocumentSource, public NeedsMergerDocumentSource {
public:
    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement spec, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    DocumentSourceExchange(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                           const boost::intrusive_ptr<Exchange> exchange,
                           size_t consumerId);

    GetNextResult getNext() final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        return {StreamType::kStreaming,
                PositionRequirement::kNone,
                HostTypeRequirement::kNone,
                DiskUseRequirement::kNoDiskUse,
                FacetRequirement::kAllowed,
                TransactionRequirement::kAllowed};
    }

    const char* getSourceName() const final;

    Value serialize(boost::optional<ExplainOptions::Verbosity> explain = boost::none) const final;

    boost::intrusive_ptr<DocumentSource> getShardSource() final {
        return this;
    }
    std::list<boost::intrusive_ptr<DocumentSource>> getMergeSources() final {
        return {this};
    }

    /**
     * Set the underlying source this source should use to get Documents from. Must not throw
     * exceptions.
     */
    void setSource(DocumentSource* source) final {
        DocumentSource::setSource(source);
        _exchange->setSource(source);
    }

    GetNextResult getNext(size_t consumerId);

    size_t consumers() const {
        return _exchange->consumers();
    }

    auto getExchange() const {
        return _exchange;
    }

private:
    boost::intrusive_ptr<Exchange> _exchange;

    const size_t _consumerId;
};

}  // namespace mongo