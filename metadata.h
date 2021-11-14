/* 2021 Nov 13 by Cao
   Introducing system table metadata struct */

#pragma once
#ifndef _METADATA_H_
#define _METADATA_H_

#include <vector>
#include "result.h"

typedef struct TableRec {
	char tablename[21];
	int  attrcount;
} TableRec;


typedef struct ColumnRec {
	//char tablename[21];
	char attrname[21];
	int  attrtype;
	int  attrlength;
	int  attroffset;
	bool ix_flag;
	char indexname[21];
} ColumnRec;


class TableMetaData {
private:
	FILE* file;

public:
	TableRec table;
	std::vector<ColumnRec> columns;

public:
	TableMetaData() {
		this->file = NULL;
		this->table = TableRec();
	}
	~TableMetaData() {
		this->close();
	}

	static Result<TableMetaData, int> open(const char* const path);
	static bool create_file(const char* const path);
	bool read();
	bool write();
	void close();
};

#endif
