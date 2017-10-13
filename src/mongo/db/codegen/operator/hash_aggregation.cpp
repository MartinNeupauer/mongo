#include "mongo/db/codegen/operator/hash_aggregation.h"
#include "mongo/db/codegen/operator/rh.h"

namespace rohan
{
	using CompareFnType = bool(*)(char*, char*);
	struct HashTableValue
	{
		using KEY_TYPE = char*;

		char* key_;

		char* key() { return key_; }
	};
	uint64_t dontusehash(char*)
	{
		return 0;
	}

	using HashTableType = rh::RH<HashTableValue, uint64_t, dontusehash>;

	static int hashTableConstruct(char* storage)
	{
		new(storage) HashTableType;

		return 0;
	}

	static char* hashTableFind(HashTableType* ht, uint64_t hash, char* key, HashTableType::CompareFn cmp)
	{
		auto found = ht->find(hash, key, cmp);
		return found ? found->key_ : nullptr;
	}

	static void hashTableInsert(HashTableType* ht, uint64_t hash, char* keyvalue)
	{
		ht->insert(hash, HashTableValue{keyvalue});
	}

	static char* hashTableEnumerate(HashTableType* ht, uint64_t* position)
	{
		auto& table = ht->table();
		auto size = table.size();
		while (*position < size)
		{
			if (table[*position]._hash & ht->hashmask())
			{
				char* value = table[*position]._value.key_;
				++ *position;
				return value;
			}
			++ *position;
		}
		return nullptr;
	}

	const anta::StructType* XteHashAgg::generateKeyStruct()
	{
		auto type = factory_.UStructType(factory_.globalScope(),"");
		std::vector<const anta::Type*> fields;

		for (auto idx : groupby_)
			fields.push_back(factory_.delinearize(input_->outputSchema()[idx]));

		type->addFields(fields);

		return type;
	}

	const anta::StructType* XteHashAgg::generateKeyValueStruct()
	{
		auto type = factory_.UStructType(factory_.globalScope(), "");

		type->addFields(envTableOpen_->types());

		return type;
	}

	anta::Function* XteHashAgg::generateKeyHashFunction(const anta::Type* keyStruct)
	{
		auto fn = function_(int64_, uniqueName("keyHashFunction")); fn->setConst(); fn->setInline();
			auto key = param_(ptr_(keyStruct), "%key");
		body_();
			auto h = var_(int64_, "h");
			h = const64_(0);
			for (unsigned i = 0; i < groupby_.size(); ++i)
			{
				if (key[i].expr_->type()->is_a<anta::StringType>())
				{
					h = h ^ eval_("hashString", { key[i] });
				}
				else if(key[i].expr_->type()->is_a<anta::BSONVariant>())
				{
					h = h ^ eval_("BSON::hashByRef", { &key[i] });
				}
				else
				{
					throw std::logic_error("unsupported type in hash");
				}
			}
			return_(h);
		end_();
		return fn;
	}

	anta::Function* XteHashAgg::generateKeyCompareFunction(const anta::Type* keyStruct)
	{
		auto fn = function_(int1_, uniqueName("keyCompareFunction")); fn->setConst(); fn->setInline();
			auto lhs = param_(ptr_(keyStruct), "%lhs");
			auto rhs = param_(ptr_(keyStruct), "%rhs");
		body_();
			for (unsigned i = 0; i < groupby_.size(); ++i)
			{
				if (lhs[i].expr_->type()->is_a<anta::StringType>())
				{
					if_(eval_("compareString", { lhs[i], rhs[i] }) != const_(0));
						return_(const1_(false));
					end_();
				}
				else if (lhs[i].expr_->type()->is_a<anta::BSONVariant>())
				{
					if_(!eval_("BSON::compareEQByRef", { &lhs[i], &rhs[i] }));
						return_(const1_(false));
					end_();
				}
				else
				{
					throw std::logic_error("unsupported type in compare");
				}
			}
			return_(const1_(true));
		end_();
		return fn;
	}

	void XteHashAgg::generate(rohan::GenContext& ctx)
	{
		rohan::GenContext inputctx(ctx);
		input_->generate(inputctx);

		auto keyStruct = generateKeyStruct();
		auto keyValueStruct = generateKeyValueStruct();
		auto keyHashFn = generateKeyHashFunction(keyStruct);
		auto keyCompareFn = generateKeyCompareFunction(keyStruct);

		auto cppHashTableConstruct = function_(int_, "cppHashTableConstruct", (void*)hashTableConstruct);
			param_(pint8_, "%this");
		end_();

		auto cppHashTableFind = function_(ptr_(keyValueStruct), "cppHashTableFind", (void*)hashTableFind);
			param_(pint8_, "%this");
			param_(int64_, "%hash");
			param_(ptr_(keyStruct), "%key");
			param_(ptr_(keyCompareFn->type()), "%cmp");
		end_();

		auto cppHashTableInsert = function_("cppHashTableInsert", (void*)hashTableInsert);
			param_(pint8_, "%this");
			param_(int64_, "%hash");
			param_(ptr_(keyValueStruct), "%keyvalue");
		end_();

		auto cppHashTableEnumerate = function_(ptr_(keyValueStruct), "cppHashAggEnumerate", (void*)hashTableEnumerate);
			param_(pint8_, "%this");
			param_(pint64_, "%iter");
		end_();

		ctx.open_ = mfunction_(uniqueName("hashAggOpen"));
			generateOpenParameters();
			generateOpenReturns();
		{
		body_();
			auto errorCodeOpen = var_(int_, "errorCodeOpen");
			auto errorCodeNextRow = var_(int_, "%errorCodeNextRow");
			auto state = var_("%state");

			// open an input
			mcall_(inputctx.open_,{ state , var_("%reopen") });
				branch_(rohan::kOpenDone);
				end_();
				generateHandleErrorCancel(errorCodeOpen);
			end_();

			auto hashTablePtr = var_("%hashAggState", state + const_(stateOffset_));

			errorCodeOpen = eval_(cppHashTableConstruct, { hashTablePtr });

			if_(errorCodeOpen);
				mreturn_(rohan::kError, { errorCodeOpen });
			end_();

			auto hashTableIter = var_("%hashAggIter", cast_(pint64_, state + const_(hashIteratorOffset_)));
			*hashTableIter = const64_(0);

			*cast_(pint64_, state + const_(hashLastKeyValOffset_)) = const64_(0);

			loop_();
				auto columns = generateColumnVariables(input_->outputSchema(), "inColumn", envIn_);

				mcall_(inputctx.nextrow_, { state });
					branch_(rohan::kNextRowDone, columns);
					end_();
					branch_(rohan::kNextRowEOS);
						mreturn_(rohan::kOpenDone);
					end_();
					generateHandleErrorCancel(errorCodeNextRow);
				end_();

				auto searchkey = var_(keyStruct, "searchkey");
				for (unsigned i = 0; i < groupby_.size(); ++i)
				{
					searchkey[i] = columns[groupby_[i]];
				}

				auto searchhash = var_("%searchhash", eval_(keyHashFn, { &searchkey }));

				auto keyval = var_("keyval", eval_(cppHashTableFind, {hashTablePtr, searchhash, &searchkey, funcptr_(keyCompareFn)}));
				for (unsigned i = 0; i < keyValueStruct->fieldCount(); ++i)
				{
					envTableOpen_->fillHole(i, keyval[i].expr_);
				}
				if_(keyval);
					// update
					paste_(aggStepStmt_);
				else_();
					// insert into a hash table
					keyval = cast_(ptr_(keyValueStruct), eval_("allocateMemory", { nullptr_(pint8_), const_(keyValueStruct->allocSize()) }));
					// copy keys
					for (unsigned i = 0; i < groupby_.size(); ++i)
					{
						keyval[i] = columns[groupby_[i]];
					}

					// initialize aggregates
					paste_(initGroupStmt_);

					call_(cppHashTableInsert, { hashTablePtr, searchhash, keyval });
				end_();

			end_();
		end_();

		if (!ctx.disableInline_)
		{
			anta::InlineMFunc inliner(factory_, *ctx.open_, { inputctx.open_, inputctx.nextrow_ });
			inliner.run();
			factory_.destroyFunction(inputctx.open_);
			factory_.destroyFunction(inputctx.nextrow_);
		}
		}

		ctx.nextrow_ = mfunction_(uniqueName("hashAggNextRow"));
			generateNextParameters();
			generateNextReturns();
		{
		body_();
			auto state = var_("%state");
			//loop over rows in a hash table
			auto hashTablePtr = var_("%hashAggState", state + const_(stateOffset_));
			auto hashTableIter = var_("%hashAggIter", cast_(pint64_, state + const_(hashIteratorOffset_)));

			auto keyval = var_("%keyval", eval_(cppHashTableEnumerate, { hashTablePtr, hashTableIter }));
			auto lastKeyValuePtr = var_("%lastkv", cast_(ptr_(ptr_(keyValueStruct)), state + const_(hashLastKeyValOffset_)));


			if_(*lastKeyValuePtr);
				call_("releaseMemory", { nullptr_(pint8_), cast_(pint8_,*lastKeyValuePtr)});
				*lastKeyValuePtr = nullptr_(ptr_(keyValueStruct));
			end_();

			if_(keyval == nullptr_(ptr_(keyValueStruct)));
				mreturn_(rohan::kNextRowEOS);
			end_();

			*lastKeyValuePtr = keyval;

			if (envTableGetRow_)
			{
				for (unsigned i = 0; i < keyValueStruct->fieldCount(); ++i)
				{
					envTableGetRow_->fillHole(i, keyval[i].expr_);
				}
			}

			auto columns = generateColumnVariables(outputSchema(), "outColumns", envOut_);

			if (finalStepStmt_)
			{
				// key columns
				for (unsigned i = 0; i < groupby_.size(); ++i)
				{
					columns[i] = keyval[i];
				}
				paste_(finalStepStmt_);
			}
			else
			{
				// key columns + agg columns
				for (unsigned i = 0; i < envOut_->size(); ++i)
				{
					columns[i] = keyval[i];
				}
			}
			mreturn_(rohan::kNextRowDone, columns);
		end_();
		}
	}

	void XteHashAgg::calculateRuntimeState(RuntimeState& state)
	{
		// align first
		state._size = anta::alignUp(state._size, alignof(HashTableType));
		stateOffset_ = state._size;
		state._size += sizeof(HashTableType);

		state._size = anta::alignUp(state._size, alignof(uint64_t));
		hashIteratorOffset_ = state._size;
		state._size += sizeof(uint64_t);

		state._size = anta::alignUp(state._size, alignof(uint64_t));
		hashLastKeyValOffset_ = state._size;
		state._size += sizeof(uint64_t);

		input_->calculateRuntimeState(state);
	}
}
