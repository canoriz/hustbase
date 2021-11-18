/* Created by Cao 2021 Nov 14
   Table metadata  */

#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include "metadata.h"

ColumnRec::ColumnRec(
	const char* const _name,
	const char* const _attrname,
	int _attrtype,
	int _attrlength,
	int _attroffset,
	bool _ix_flag,
	const char* const _index_name
) {
	strcpy(this->tablename, _name);
	strcpy(this->attrname, _attrname);
	this->attrtype = _attrtype;
	this->attrlength = _attrlength;
	this->attroffset = _attroffset;
	this->ix_flag = _ix_flag;
	strcpy(this->indexname, _index_name);
}

ColumnRec::ColumnRec() {
	strcpy(this->tablename, "");
	strcpy(this->attrname, "");
	this->attrtype = 0;
	this->attrlength = 0;
	this->attroffset = 0;
	this->ix_flag = false;
	strcpy(this->indexname, "");
}


Result<TableMetaData, int> TableMetaData::open(const char* const path) {
	TableMetaData metadata;
	metadata.file = fopen(path, "rb");
	if (metadata.file != NULL) {
		return Result<TableMetaData, int>::Ok(metadata);
	}
	return Result<TableMetaData, int>::Err(0);
}

bool TableMetaData::create_file(const char* const path) {
	FILE* fp = fopen(path, "r");
	if (fp) {
		// already exist
		fclose(fp);
		return false;
	}
	fp = fopen(path, "wb");
	if (fp) {
		fclose(fp);
		return true;
	}
	return false;
}

bool TableMetaData::read() {
	if (1 != fread(&this->table, sizeof(this->table), 1, this->file)) {
		return false;
	}
	ColumnRec new_rec;
	while (1 == fread(&new_rec, sizeof(new_rec), 1, this->file)) {
		this->columns.push_back(new_rec);
	}
	this->close();
	return true;
}

bool TableMetaData::write(const char* const path) {
	this->close();
	this->file = fopen(path, "wb");
	if (!this->file) {
		// not open
		return false;
	}

	if (1 != fwrite(&this->table, sizeof(this->table), 1, this->file)) {
		// write failed
		return false;
	}
	for (auto const& column : this->columns) {
		int wrote = fwrite(&column, sizeof(column), 1, this->file);
		if (wrote != 1) {
			// write failed
			return false;
		}
	}

	this->close();
	return true;
}

void TableMetaData::close() {
	if (this->file) {
		fclose(this->file);
		this->file = NULL;
	}
}

Result<IndexMetaData, int> IndexMetaData::open(const char* const path) {
	IndexMetaData metadata;
	metadata.file = fopen(path, "a+b");
	if (metadata.file != NULL) {
		return Result<IndexMetaData, int>::Ok(metadata);
	}
	return Result<IndexMetaData, int>::Err(0);
}

bool IndexMetaData::create_file(const char* const path) {
	FILE* fp = fopen(path, "r");
	if (fp) {
		// already exist
		fclose(fp);
		return false;
	}
	fp = fopen(path, "wb");
	if (fp) {
		fclose(fp);
		return true;
	}
	return false;
}

bool IndexMetaData::read() {
	if (1 != fread(&this->index, sizeof(this->index), 1, this->file)) {
		return false;
	}
	return true;
}

bool IndexMetaData::write() {
	if (!this->file) {
		// not open
		return false;
	}

	if (1 != fwrite(&this->index, sizeof(this->index), 1, this->file)) {
		// write failed
		return false;
	}
	this->close();
	return true;
}

void IndexMetaData::close() {
	if (this->file) {
		fclose(this->file);
		this->file = NULL;
	}
}
