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

#include "mongo/platform/basic.h"

#include "mongo/db/exec/sbe/stages/exchange.h"

#include "mongo/base/init.h"
#include "mongo/db/client.h"

namespace mongo::sbe {
std::unique_ptr<ThreadPool> s_globalThreadPool;
MONGO_INITIALIZER(s_globalThreadPool)(InitializerContext* context) {
    ThreadPool::Options options;
    options.poolName = "parallel execution pool";
    options.threadNamePrefix = "ExchProd";
    options.minThreads = 0;
    options.maxThreads = 128;
    options.onCreateThread = [](const std::string& name) { Client::initThread(name); };
    s_globalThreadPool = std::make_unique<ThreadPool>(options);
    s_globalThreadPool->startup();

    return Status::OK();
}

ExchangePipe::ExchangePipe(size_t size) {
    // all buffers start empty
    _fullCount = 0;
    _emptyCount = size;
    for (size_t i = 0; i < _emptyCount; ++i) {
        _fullBuffers.emplace_back(nullptr);
        _emptyBuffers.emplace_back(std::make_unique<ExchangeBuffer>());
    }

    // add a sentinel
    _fullBuffers.emplace_back(nullptr);
}

void ExchangePipe::close() {
    std::unique_lock lock(_mutex);

    _closed = true;

    _cond.notify_all();
}

std::unique_ptr<ExchangeBuffer> ExchangePipe::getEmptyBuffer() {
    std::unique_lock lock(_mutex);

    _cond.wait(lock, [this]() { return _closed || _emptyCount > 0; });

    if (_closed) {
        return nullptr;
    }

    --_emptyCount;

    return std::move(_emptyBuffers[_emptyCount]);
}

std::unique_ptr<ExchangeBuffer> ExchangePipe::getFullBuffer() {
    std::unique_lock lock(_mutex);

    _cond.wait(lock, [this]() { return _closed || _fullCount != _fullPosition; });

    if (_closed) {
        return nullptr;
    }

    auto pos = _fullPosition;
    _fullPosition = (_fullPosition + 1) % _fullBuffers.size();

    return std::move(_fullBuffers[pos]);
}

void ExchangePipe::putEmptyBuffer(std::unique_ptr<ExchangeBuffer> b) {
    std::unique_lock lock(_mutex);

    _emptyBuffers[_emptyCount] = std::move(b);

    ++_emptyCount;

    _cond.notify_all();
}

void ExchangePipe::putFullBuffer(std::unique_ptr<ExchangeBuffer> b) {
    std::unique_lock lock(_mutex);

    _fullBuffers[_fullCount] = std::move(b);

    _fullCount = (_fullCount + 1) % _fullBuffers.size();

    _cond.notify_all();
}

ExchangeState::ExchangeState(size_t numOfProducers,
                             value::SlotVector fields,
                             ExchangePolicy policy,
                             std::unique_ptr<EExpression> partition,
                             std::unique_ptr<EExpression> orderLess)
    : _policy(policy),
      _numOfProducers(numOfProducers),
      _fields(std::move(fields)),
      _partition(std::move(partition)),
      _orderLess(std::move(orderLess)) {}

ExchangePipe* ExchangeState::pipe(size_t consumerTid, size_t producerTid) {
    return _consumers[consumerTid]->pipe(producerTid);
}

ExchangeBuffer* ExchangeConsumer::getBuffer(size_t producerId) {
    if (_fullBuffers[producerId]) {
        return _fullBuffers[producerId].get();
    }

    _fullBuffers[producerId] = _pipes[producerId]->getFullBuffer();

    return _fullBuffers[producerId].get();
}

void ExchangeConsumer::putBuffer(size_t producerId) {
    if (!_fullBuffers[producerId]) {
        uasserted(ErrorCodes::InternalError, "get not called before put");
    }

    // clear the buffer before putting it back on the empty (free) list
    _fullBuffers[producerId]->clear();

    _pipes[producerId]->putEmptyBuffer(std::move(_fullBuffers[producerId]));
}

ExchangeConsumer::ExchangeConsumer(std::unique_ptr<PlanStage> input,
                                   size_t numOfProducers,
                                   value::SlotVector fields,
                                   ExchangePolicy policy,
                                   std::unique_ptr<EExpression> partition,
                                   std::unique_ptr<EExpression> orderLess)
    : PlanStage("exchange"_sd) {
    _children.emplace_back(std::move(input));
    _state = std::make_shared<ExchangeState>(
        numOfProducers, std::move(fields), policy, std::move(partition), std::move(orderLess));

    _tid = _state->addConsumer(this);
    _orderPreserving = _state->isOrderPreserving();
}
ExchangeConsumer::ExchangeConsumer(std::shared_ptr<ExchangeState> state)
    : PlanStage("exchange"_sd), _state(state) {
    _tid = _state->addConsumer(this);
    _orderPreserving = _state->isOrderPreserving();
}
std::unique_ptr<PlanStage> ExchangeConsumer::clone() {
    return std::make_unique<ExchangeConsumer>(_state);
}
void ExchangeConsumer::prepare(CompileCtx& ctx) {
    for (size_t idx = 0; idx < _state->fields().size(); ++idx) {
        _outgoing.emplace_back(ExchangeBuffer::Accessor{});
    }
    // compile '<' function
}
value::SlotAccessor* ExchangeConsumer::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    // accessors to pipes
    for (size_t idx = 0; idx < _state->fields().size(); ++idx) {
        if (_state->fields()[idx] == slot) {
            return &_outgoing[idx];
        }
    }

    // correlated values and stuff
    return ctx.getAccessor(slot);
}
void ExchangeConsumer::open(bool reOpen) {
    _commonStats.opens++;

    if (reOpen) {
        uasserted(ErrorCodes::InternalError, "exchange consumer cannot be reopened");
    }

    {
        std::unique_lock lock(_state->consumerOpenMutex());
        bool allConsumers = (++_state->consumerOpen()) == _state->numOfConsumers();

        // create all pipes
        if (_orderPreserving) {
            for (size_t idx = 0; idx < _state->numOfProducers(); ++idx) {
                _pipes.emplace_back(std::make_unique<ExchangePipe>(2));
                _fullBuffers.emplace_back(nullptr);
                _bufferPos.emplace_back(0);
            }
        } else {
            _pipes.emplace_back(std::make_unique<ExchangePipe>(_state->numOfProducers() * 2));
            _fullBuffers.emplace_back(nullptr);
            _bufferPos.emplace_back(0);
        }
        _eofs = 0;

        if (_tid == 0) {
            // Consumer ID 0

            // wait for all other consumers to show up
            if (!allConsumers) {
                _state->consumerOpenCond().wait(
                    lock, [this]() { return _state->consumerOpen() == _state->numOfConsumers(); });
            }

            // clone n copies of the subtree for every producer

            PlanStage* masterSubTree = _children[0].get();
            masterSubTree->detachFromOperationContext();

            for (size_t idx = 0; idx < _state->numOfProducers(); ++idx) {
                if (idx == 0) {
                    _state->producerPlans().emplace_back(
                        std::make_unique<ExchangeProducer>(std::move(_children[0]), _state));
                    // We have moved the child to the producer so clear the children vector.
                    _children.clear();
                } else {
                    _state->producerPlans().emplace_back(
                        std::make_unique<ExchangeProducer>(masterSubTree->clone(), _state));
                }
            }

            // start n producers
            for (size_t idx = 0; idx < _state->numOfProducers(); ++idx) {
                auto pf = makePromiseFuture<void>();
                s_globalThreadPool->schedule(
                    [this, idx, promise = std::move(pf.promise)](auto status) mutable {
                        invariant(status);

                        auto opCtx = cc().makeOperationContext();

                        promise.setWith([&] {
                            ExchangeProducer::start(opCtx.get(),
                                                    std::move(_state->producerPlans()[idx]));
                        });
                    });
                _state->addProducerFuture(std::move(pf.future));
            }
        } else {
            // Consumer ID >0

            // make consumer 0 know that this consumer has shown up
            if (allConsumers) {
                _state->consumerOpenCond().notify_all();
            }
        }
    }

    {
        std::unique_lock lock(_state->consumerOpenMutex());
        if (_tid == 0) {
            // signal all other consumers that the open is done
            _state->consumerOpen() = 0;
            _state->consumerOpenCond().notify_all();
        } else {
            // wait for the open to be done
            _state->consumerOpenCond().wait(lock, [this]() { return _state->consumerOpen() == 0; });
        }
    }
}

PlanState ExchangeConsumer::getNext() {
    if (_orderPreserving) {
        // build a heap and return min element
        uasserted(ErrorCodes::InternalErrorNotSupported, "ordere exchange not yet implemented");
    } else {
        while (_eofs < _state->numOfProducers()) {
            auto buffer = getBuffer(0);
            if (!buffer) {
                // early out
                return trackPlanState(PlanState::IS_EOF);
            }
            if (_bufferPos[0] < buffer->count()) {
                // we still return from the current buffer
                for (size_t idx = 0; idx < _outgoing.size(); ++idx) {
                    _outgoing[idx].setBuffer(buffer);
                    _outgoing[idx].setIndex(_bufferPos[0] * _state->fields().size() + idx);
                }
                ++_bufferPos[0];
                ++_rowProcessed;
                return trackPlanState(PlanState::ADVANCED);
            }

            if (buffer->isEof()) {
                ++_eofs;
            }

            putBuffer(0);
            _bufferPos[0] = 0;
        }
    }
    return trackPlanState(PlanState::IS_EOF);
}
void ExchangeConsumer::close() {
    _commonStats.closes++;

    {
        std::unique_lock lock(_state->consumerCloseMutex());
        ++_state->consumerClose();

        // signal early out
        for (auto& p : _pipes) {
            p->close();
        }

        if (_tid == 0) {
            // Consumer ID 0
            // wait for n producers to finish
            for (size_t idx = 0; idx < _state->numOfProducers(); ++idx) {
                _state->producerResults()[idx].wait();
            }
        }

        if (_state->consumerClose() == _state->numOfConsumers()) {
            // signal all other consumers that the close is done
            _state->consumerCloseCond().notify_all();
        } else {
            // wait for the close to be done
            _state->consumerCloseCond().wait(
                lock, [this]() { return _state->consumerClose() == _state->numOfConsumers(); });
        }
    }
    // rethrow the first stored exception from producers
    // we can do it outside of the lock as everybody else is gone by now
    if (_tid == 0) {
        // Consumer ID 0
        for (size_t idx = 0; idx < _state->numOfProducers(); ++idx) {
            _state->producerResults()[idx].get();
        }
    }
}

std::unique_ptr<PlanStageStats> ExchangeConsumer::getStats() const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);
    ret->children.emplace_back(_children[0]->getStats());
    return ret;
}

const SpecificStats* ExchangeConsumer::getSpecificStats() const {
    return nullptr;
}

std::vector<DebugPrinter::Block> ExchangeConsumer::debugPrint() {
    std::vector<DebugPrinter::Block> ret;
    DebugPrinter::addKeyword(ret, "exchange");
    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _state->fields().size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _state->fields()[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    ret.emplace_back(std::to_string(_state->numOfProducers()));

    switch (_state->policy()) {
        case ExchangePolicy::broadcast:
            DebugPrinter::addKeyword(ret, "bcast");
            break;
        case ExchangePolicy::roundrobin:
            DebugPrinter::addKeyword(ret, "round");
            break;
        default:
            uasserted(ErrorCodes::InternalErrorNotSupported, "policy not yet implemented");
    }

    DebugPrinter::addNewLine(ret);
    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());

    return ret;
}

ExchangePipe* ExchangeConsumer::pipe(size_t producerTid) {
    if (_orderPreserving) {
        return _pipes[producerTid].get();
    } else {
        return _pipes[0].get();
    }
}

ExchangeBuffer* ExchangeProducer::getBuffer(size_t consumerId) {
    if (_emptyBuffers[consumerId]) {
        return _emptyBuffers[consumerId].get();
    }

    _emptyBuffers[consumerId] = _pipes[consumerId]->getEmptyBuffer();

    if (!_emptyBuffers[consumerId]) {
        closePipes();
    }

    return _emptyBuffers[consumerId].get();
}

void ExchangeProducer::putBuffer(size_t consumerId) {
    if (!_emptyBuffers[consumerId]) {
        uasserted(ErrorCodes::InternalError, "get not called before put");
    }

    _pipes[consumerId]->putFullBuffer(std::move(_emptyBuffers[consumerId]));
}

void ExchangeProducer::closePipes() {
    for (auto& p : _pipes) {
        p->close();
    }
}

ExchangeProducer::ExchangeProducer(std::unique_ptr<PlanStage> input,
                                   std::shared_ptr<ExchangeState> state)
    : PlanStage("exchangep"_sd), _state(state) {
    _children.emplace_back(std::move(input));

    _tid = _state->addProducer(this);

    // retrieve the correct pipes
    for (size_t idx = 0; idx < _state->numOfConsumers(); ++idx) {
        _pipes.emplace_back(_state->pipe(idx, _tid));
        _emptyBuffers.emplace_back(nullptr);
    }
}

void ExchangeProducer::start(OperationContext* opCtx, std::unique_ptr<PlanStage> producer) {
    ExchangeProducer* p = static_cast<ExchangeProducer*>(producer.get());

    p->attachFromOperationContext(opCtx);

    try {
        CompileCtx ctx;
        p->prepare(ctx);
        p->open(false);

        auto status = p->getNext();
        if (status != PlanState::IS_EOF) {
            uasserted(ErrorCodes::InternalError, "producer returned invalid state");
        }

        p->close();
    } catch (...) {
        // this is sketchy but close the pipes as minimum
        p->closePipes();
        throw;
    }
}

std::unique_ptr<PlanStage> ExchangeProducer::clone() {
    uasserted(ErrorCodes::InternalError, "ExchangeProducer is not cloneable");
}

void ExchangeProducer::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);
    for (auto& f : _state->fields()) {
        _incoming.emplace_back(_children[0]->getAccessor(ctx, f));
    }
}
value::SlotAccessor* ExchangeProducer::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    return _children[0]->getAccessor(ctx, slot);
}
void ExchangeProducer::open(bool reOpen) {
    _commonStats.opens++;
    if (reOpen) {
        uasserted(ErrorCodes::InternalError, "exchange producer cannot be reopened");
    }
    _children[0]->open(reOpen);
}
bool ExchangeProducer::appendData(size_t consumerId) {
    auto buffer = getBuffer(consumerId);
    // detect early out
    if (!buffer) {
        return false;
    }
    // copy data to buffer
    if (buffer->appendData(_incoming)) {
        // send it off to consumer when full
        putBuffer(consumerId);
    }

    return true;
}

PlanState ExchangeProducer::getNext() {
    while (_children[0]->getNext() == PlanState::ADVANCED) {
        // push to the correct pipe
        switch (_state->policy()) {
            case ExchangePolicy::broadcast: {
                for (size_t idx = 0; idx < _pipes.size(); ++idx) {
                    // detect early out in the loop
                    if (!appendData(idx)) {
                        return trackPlanState(PlanState::IS_EOF);
                    }
                }
            } break;
            case ExchangePolicy::roundrobin: {
                // detect early out in the loop
                if (!appendData(_roundRobinCounter)) {
                    return trackPlanState(PlanState::IS_EOF);
                }
                _roundRobinCounter = (_roundRobinCounter + 1) % _pipes.size();
            } break;
            case ExchangePolicy::partition: {
                uasserted(ErrorCodes::InternalErrorNotSupported, "policy not yet implemented");
            } break;
            default:
                MONGO_UNREACHABLE;
                break;
        }
    }

    // send off partially filled buffers and eof marker
    for (size_t idx = 0; idx < _pipes.size(); ++idx) {
        auto buffer = getBuffer(idx);
        // detect early out in the loop
        if (!buffer) {
            return trackPlanState(PlanState::IS_EOF);
        }
        buffer->markEof();
        // send it off to consumer
        putBuffer(idx);
    }
    return trackPlanState(PlanState::IS_EOF);
}
void ExchangeProducer::close() {
    _commonStats.closes++;
    _children[0]->close();
}

std::unique_ptr<PlanStageStats> ExchangeProducer::getStats() const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);
    ret->children.emplace_back(_children[0]->getStats());
    return ret;
}

const SpecificStats* ExchangeProducer::getSpecificStats() const {
    return nullptr;
}

std::vector<DebugPrinter::Block> ExchangeProducer::debugPrint() {
    return std::vector<DebugPrinter::Block>();
}
bool ExchangeBuffer::appendData(std::vector<value::SlotAccessor*>& data) {
    ++_count;
    for (auto accesor : data) {
        auto [tag, val] = accesor->copyOrMoveValue();
        value::ValueGuard guard{tag, val};
        _typeTags.push_back(tag);
        _values.push_back(val);
        guard.reset();
    }

    // a simply heuristic for now
    return isFull();
}
}  // namespace mongo::sbe
