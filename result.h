/* 2021 Nov 13 by Cao
   Introducing the Result<T> struct */

#pragma once
#ifndef _RESULT_H_
#define _RESULT_H_

template<typename T>
struct Result {
	bool ok;
	T result;
	Result(bool _ok, T _result) {
		this->ok = _ok;
		this->result = _result;
	}
	Result(T _result) {
		this->ok = true;
		this->result = _result;
	}
	Result() {
		this->ok = false;
		this->result = T();
	}
	static Result<T> Ok(T _result) {
		return Result<T>(_result);
	}
	static Result<T> Err() {
		return Result<T>();
	}
};

#endif
