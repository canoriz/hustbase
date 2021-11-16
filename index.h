/* index.h
   created by Cao 2021 Nov 16

   wrapper of IX_Manager */

#pragma once
#ifndef _INDEX_H_
#define _INDEX_H_

#include "RC.h"
#include "result.h"
#include "metadata.h"
#include "IX_Manager.h"

class Index {
private:
	IndexMetaData meta;

public:
	char name[21];
	char table[21];
	char column[21];
	IX_IndexHandle file;

public:
	Index() :name("") {}
	static Result<bool, RC>  create(char* path, char* iname, char* tname, char* cname, AttrType atype, int attrlen);
	static Result<Index, RC> open(char* path, char* name);

public:
	bool close();
};

#endif
