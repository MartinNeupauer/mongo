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

#include "mongo/db/exec/sbe/expressions/expression.h"
#include "mongo/db/exec/sbe/stages/stages.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/future.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/future.h"

#include <vector>

namespace mongo {
namespace sbe {
class ExchangeConsumer;
class ExchangeProducer;

enum class ExchangePolicy { broadcast, roundrobin, partition };

// A unit of exchange between a consumer and a producer
class ExchangeBuffer {
    std::vector<value::TypeTags> _typeTags;
    std::vector<value::Value> _values;

    // mark that this is the last buffer
    bool _eof{false};
    size_t _count{0};

    friend class Accessor;

public:
    ~ExchangeBuffer() {
        clear();
    }
    void clear() {
        _eof = false;
        _count = 0;

        for (size_t idx = 0; idx < _typeTags.size(); ++idx) {
            value::releaseValue(_typeTags[idx], _values[idx]);
        }
        _typeTags.clear();
        _values.clear();
    }
    void markEof() {
        _eof = true;
    }
    auto isEof() const {
        return _eof;
    }

    bool appendData(std::vector<value::SlotAccessor*>& data);
    auto count() const {
        return _count;
    }
    // bogus for now
    auto isFull() const {
        return _typeTags.size() >= 10240 || _count >= 1024;
    }

    class Accessor : public value::SlotAccessor {
        ExchangeBuffer* _buffer{nullptr};
        size_t _index{0};

    public:
        void setBuffer(ExchangeBuffer* buffer) {
            _buffer = buffer;
        }
        void setIndex(size_t index) {
            _index = index;
        }

        // Return non-owning view of the value
        std::pair<value::TypeTags, value::Value> getViewOfValue() const override {
            return {_buffer->_typeTags[_index], _buffer->_values[_index]};
        }
        std::pair<value::TypeTags, value::Value> copyOrMoveValue() override {
            auto tag = _buffer->_typeTags[_index];
            auto val = _buffer->_values[_index];

            _buffer->_typeTags[_index] = value::TypeTags::Nothing;

            return {tag, val};
        }
    };
};

// A connection that moves data between a consumer and a producer
class ExchangePipe {
    Mutex _mutex = MONGO_MAKE_LATCH("ExchangePipe::_mutex");
    stdx::condition_variable _cond;

    std::vector<std::unique_ptr<ExchangeBuffer>> _fullBuffers;
    std::vector<std::unique_ptr<ExchangeBuffer>> _emptyBuffers;
    size_t _fullCount{0};
    size_t _fullPosition{0};
    size_t _emptyCount{0};

    // early out - pipe closed
    bool _closed{false};

public:
    ExchangePipe(size_t size);

    void close();
    std::unique_ptr<ExchangeBuffer> getEmptyBuffer();
    std::unique_ptr<ExchangeBuffer> getFullBuffer();
    void putEmptyBuffer(std::unique_ptr<ExchangeBuffer>);
    void putFullBuffer(std::unique_ptr<ExchangeBuffer>);
};

// Common shared state between all consumers and producers of a single exchange
class ExchangeState {
    const ExchangePolicy _policy;
    const size_t _numOfProducers;
    std::vector<ExchangeConsumer*> _consumers;
    std::vector<ExchangeProducer*> _producers;
    std::vector<std::unique_ptr<PlanStage>> _producerPlans;
    std::vector<Future<void>> _producerResults;

    // variables (fields) that pass through the exchange
    const std::vector<std::string> _fields;

    // partitioning function
    const std::unique_ptr<EExpression> _partition;

    // the '<' function for order preserving exchange
    const std::unique_ptr<EExpression> _orderLess;

    // This is verbose and heavyweight. Recondsider something lighter
    // at minimum try to share a single mutex (i.e. _stateMutex) if safe
    std::mutex _consumerOpenMutex;
    std::condition_variable _consumerOpenCond;
    size_t _consumerOpen{0};

    std::mutex _consumerCloseMutex;
    std::condition_variable _consumerCloseCond;
    size_t _consumerClose{0};

public:
    ExchangeState(size_t numOfProducers,
                  const std::vector<std::string>& fields,
                  ExchangePolicy policy,
                  std::unique_ptr<EExpression> partition,
                  std::unique_ptr<EExpression> orderLess);

    bool isOrderPreserving() const {
        return !!_orderLess;
    }
    auto policy() const {
        return _policy;
    }

    size_t addConsumer(ExchangeConsumer* c) {
        _consumers.push_back(c);
        return _consumers.size() - 1;
    }

    size_t addProducer(ExchangeProducer* p) {
        _producers.push_back(p);
        return _producers.size() - 1;
    }

    void addProducerFuture(Future<void> f) {
        _producerResults.emplace_back(std::move(f));
    }

    auto& consumerOpenMutex() {
        return _consumerOpenMutex;
    }
    auto& consumerOpenCond() {
        return _consumerOpenCond;
    }
    auto& consumerOpen() {
        return _consumerOpen;
    }

    auto& consumerCloseMutex() {
        return _consumerCloseMutex;
    }
    auto& consumerCloseCond() {
        return _consumerCloseCond;
    }
    auto& consumerClose() {
        return _consumerClose;
    }

    auto& producerPlans() {
        return _producerPlans;
    }
    auto& producerResults() {
        return _producerResults;
    }

    auto numOfConsumers() const {
        return _consumers.size();
    }
    auto numOfProducers() const {
        return _numOfProducers;
    }

    auto& fields() const {
        return _fields;
    }
    ExchangePipe* pipe(size_t consumerTid, size_t producerTid);
};

class ExchangeConsumer final : public PlanStage {
    std::shared_ptr<ExchangeState> _state;
    size_t _tid{0};

    // accessors for the outgoing values (from the exchange buffers)
    std::vector<ExchangeBuffer::Accessor> _outgoing;

    // pipes to producers (if order preserving) or a single pipe shared by all producers
    std::vector<std::unique_ptr<ExchangePipe>> _pipes;
    // current full buffers that this consumer is processing
    std::vector<std::unique_ptr<ExchangeBuffer>> _fullBuffers;
    // current position in buffers that this consumer is processing
    std::vector<size_t> _bufferPos;
    // count how may EOFs we have seen so far
    size_t _eofs{0};

    bool _orderPreserving{false};

    ExchangeBuffer* getBuffer(size_t producerId);
    void putBuffer(size_t producerId);

    size_t _rowProcessed{0};

public:
    ExchangeConsumer(std::unique_ptr<PlanStage> input,
                     size_t numOfProducers,
                     const std::vector<std::string>& fields,
                     ExchangePolicy policy,
                     std::unique_ptr<EExpression> partition,
                     std::unique_ptr<EExpression> orderLess);

    ExchangeConsumer(std::shared_ptr<ExchangeState> state);

    std::unique_ptr<PlanStage> clone() override;

    void prepare(CompileCtx& ctx) override;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, std::string_view field) override;
    void open(bool reOpen) override;
    PlanState getNext() override;
    void close() override;

    std::vector<DebugPrinter::Block> debugPrint() override;

    ExchangePipe* pipe(size_t producerTid);
};

class ExchangeProducer final : public PlanStage {
    std::shared_ptr<ExchangeState> _state;
    size_t _tid{0};
    size_t _roundRobinCounter{0};

    std::vector<value::SlotAccessor*> _incoming;

    std::vector<ExchangePipe*> _pipes;

    // current empty buffers that this producer is processing
    std::vector<std::unique_ptr<ExchangeBuffer>> _emptyBuffers;

    ExchangeBuffer* getBuffer(size_t consumerId);
    void putBuffer(size_t consumerId);

    void closePipes();
    bool appendData(size_t consumerId);

public:
    ExchangeProducer(std::unique_ptr<PlanStage> input, std::shared_ptr<ExchangeState> state);

    static void start(OperationContext* opCtx, std::unique_ptr<PlanStage> producer);

    std::unique_ptr<PlanStage> clone() override;

    void prepare(CompileCtx& ctx) override;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, std::string_view field) override;
    void open(bool reOpen) override;
    PlanState getNext() override;
    void close() override;

    std::vector<DebugPrinter::Block> debugPrint() override;
};
}  // namespace sbe
}  // namespace mongo