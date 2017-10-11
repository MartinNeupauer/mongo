#pragma once
#include <assert.h>
#include <stdint.h>
#include <vector>

namespace rh
{
	template<typename T, typename HASHTYPE, HASHTYPE(*HASHFN)(typename T::KEY_TYPE)>
	class RH
	{
		struct Bucket
		{
			// a stored hash value of a key - it must be never 0 as it indicates an empty slot
			HASHTYPE _hash;
#ifdef RH_DBG
			uint32_t _dib; // for debugging only !!!!
			size_t _initial; // for debugging only !!!!
#endif
			T _value;

			void reset()
			{
				_hash = 0;
#ifdef RH_DBG
				_dib = 0;
				_initial = 0;
#endif
			}
		};

		std::vector<Bucket> _table;
		uint32_t _count;
		size_t _maxdib;

		auto mask() const { return _table.size() - 1; }

		size_t hash_to_index(HASHTYPE hash) const
		{
			size_t index = hash & mask();
			return index;
		}

		HASHTYPE hash_function(typename T::KEY_TYPE k) const
		{
			HASHTYPE ret = HASHFN(k);
			ret |= hashmask();
			return ret;
		}

		// distance to an initial bucket (dib)
		size_t compute_dib(size_t position) const
		{
			return compute_dib(_table[position]._hash, position);
		}

		size_t compute_dib(HASHTYPE hash, size_t position) const
		{
			size_t initial = hash_to_index(hash);

			return compute_delta(initial, position);
		}

		// compute (position - initial) modulo table size
		size_t compute_delta(size_t initial, size_t position) const
		{
			if (position >= initial)
			{
				return position - initial;
			}
			else
			{
				// wrap around
				return position + (_table.size() - initial);
			}
		}
		void advance(size_t& position) const
		{
			++position;
			if (position == _table.size())
				position = 0;
		}

		size_t left_shift(size_t position)
		{
			Bucket saved_bucket;
			saved_bucket = _table[position];
			size_t shifted = 0;

			for (;;)
			{
				++shifted;
				advance(position);

				if (is_null(position))
				{
					_table[position] = saved_bucket;
#ifdef RH_DBG
					_table[position]._dib = (uint32_t)compute_dib(position); // DBG only !!!
#endif
					break;
				}

				// swap
				std::swap(_table[position], saved_bucket);
#ifdef RH_DBG
				_table[position]._dib = (uint32_t)compute_dib(position); // DBG only !!!
#endif
			}

			return shifted;
		}

		bool is_null(size_t position) const
		{
			return (_table[position]._hash & hashmask()) == 0;
		}

	public:
		HASHTYPE hashmask() const { return ((HASHTYPE)1) << (sizeof(HASHTYPE) * 8 - 1); }

		RH() : _count(0), _maxdib(0)
		{
			_table.resize(8);
			for (auto& b : _table)
				b.reset();
		}

		auto count() const { return _count; }
		auto& table() { return _table; }

		void grow()
		{
			std::vector<Bucket> newTable;
			newTable.resize(_table.size() * 2);
			for (auto& b : newTable)
				b.reset();

			swap(newTable, _table);
			_count = 0;
			_maxdib = 0;

			for (auto& b : newTable) {
				if (b._hash & hashmask()) {
					insert(b._hash, b._value);
				}
			}
		}

		void insert(const T& v)
		{
			HASHTYPE hash = hash_function(v.key());
			insert(hash, v);
		}
		void insert(HASHTYPE hash, const T& v)
		{
			hash |= hashmask();

			assert(_count < _table.size());
			if (_table.size() * 3 / 4 <= _count)
				grow();

			++_count;

			size_t position = hash_to_index(hash);
			size_t initial = position;
			size_t shifted = 0;

			for (;;)
			{
				if (is_null(position))
				{
					break;
				}
				else if (compute_delta(initial, position) > compute_dib(position))
				{
					shifted = left_shift(position);
					break;
				}

				advance(position);
			}

			if (compute_delta(initial, position) > _maxdib)
				_maxdib = compute_delta(initial, position);

			_table[position]._hash = hash;
			_table[position]._value = v;
#ifdef RH_DBG
			_table[position]._dib = (uint32_t)compute_delta(initial, position); // DBG only !!!!
			_table[position]._initial = initial; //DBG only !!!
#endif
		}

		size_t find_position(HASHTYPE hash, typename T::KEY_TYPE key)
		{
			size_t position = hash_to_index(hash);
			size_t initial = position;

			for (;;)
			{
				if (is_null(position) || compute_delta(initial, position) > compute_dib(position))
				{
					return _table.size();
				}
				else if (_table[position]._hash == hash && _table[position]._value.key() == key)
				{
					return position;
				}

				advance(position);
			}
		}

		using CompareFn = bool(*)(typename T::KEY_TYPE, typename T::KEY_TYPE);
		size_t find_position(HASHTYPE hash, typename T::KEY_TYPE key, CompareFn fn)
		{
			size_t position = hash_to_index(hash);
			size_t initial = position;

			for (;;)
			{
				if (is_null(position) || compute_delta(initial, position) > compute_dib(position))
				{
					return _table.size();
				}
				else if (_table[position]._hash == hash && fn(_table[position]._value.key(),key)/*_table[position]._value.key() == key*/)
				{
					return position;
				}

				advance(position);
			}
		}

		size_t find_position(typename T::KEY_TYPE key, CompareFn fn)
		{
			HASHTYPE hash = hash_function(key);

			return find_position(hash, key, fn);
		}

		T* find(HASHTYPE hash, typename T::KEY_TYPE key, CompareFn fn)
		{
			hash |= hashmask();
			size_t position = find_position(hash, key, fn);
			if (position == _table.size())
				return nullptr;

			return &_table[position]._value;
		}

		T* find(typename T::KEY_TYPE key)
		{
			HASHTYPE hash = hash_function(key);
			return find(hash, key, [](typename T::KEY_TYPE key1, typename T::KEY_TYPE key2) { return key1 == key2; });
		}
	};

}