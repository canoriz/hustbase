/* table.h
   created by Cao 2021 Nov 14
   
   wrapper of RM_Manager */
#pragma once
#ifndef _TABLE_H_
#define _TABLE_H_

#include "RC.h"
#include "result.h"
#include "metadata.h"
#include "RM_Manager.h"


class Table {
private:
	TableMetaData meta;
public:
	char name[21];
	RM_FileHandle file;

public:
	Table() :name("") {}
	static Result<bool, RC>  create(char* path, char* name, int sz);
	static Result<Table, RC> open(char* path, char* name);

public:
	bool close();
	bool destroy();
};


#endif
