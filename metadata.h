/* 2021 Nov 13 by Cao
   Introducing system table metadata struct */

#pragma once
#ifndef _METADATA_H_
#define _METADATA_H_

#include <vector>
#include "result.h"

typedef struct IndexRec {
	char indexname[21];
	char tablename[21];
	char columnname[21];
} IndexRec;

typedef struct TableRec {
	char tablename[21];
	int  attrcount;
	int  size;
} TableRec;

#pragma pack(1)
typedef struct ColumnRec {
	char tablename[21];
	char attrname[21];
	int  attrtype;
	int  attrlength;
	int  attroffset;
	bool ix_flag;
	char indexname[21];

	ColumnRec(
		const char* const name,
		const char* const _attrname,
		int _attrtype,
		int _attrlength,
		int _attroffset,
		bool _ix_flag,
		const char* const _index_name
	);
	ColumnRec();

} ColumnRec;
#pragma pack()


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

	static Result<TableMetaData, int> open(const char* const path);
	static bool create_file(const char* const path);
	bool read();
	bool write(const char* const path);
	void close();
};

class IndexMetaData {
private:
	FILE* file;

public:
	IndexRec index;

public:
	IndexMetaData() {
		this->file = NULL;
		this->index = IndexRec();
	}

	static Result<IndexMetaData, int> open(const char* const path);
	static bool create_file(const char* const path);
	bool read();
	bool write();
	void close();
};

#endif
