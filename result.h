/* 2021 Nov 13 by Cao
   Introducing the Result<T> struct */

#pragma once
#ifndef _RESULT_H_
#define _RESULT_H_


template<typename T, typename E>
struct Result {
	bool ok;
	T result;
	E err;
	Result(bool _ok, const T& _result, E _err) {
		this->ok     = _ok;
		this->result = _result;
		this->err    = _err;
	}
	Result(const T& _result) {
		this->ok     = true;
		this->result = _result;
		this->err    = E();
	}
	Result() {
		this->ok     = false;
		this->result = T();
		this->err    = E();
	}
	static Result<T, E> Ok(const T& _result) {
		return Result<T, E>(_result);
	}
	static Result<T, E> Err(E _err) {
		return Result<T, E>(false, T(), _err);
	}
};

#endif
