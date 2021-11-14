/* Created by Cao 2021 Nov 14
   Table metadata  */

#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include "metadata.h"


Result<TableMetaData, int> TableMetaData::open(const char* const path) {
	TableMetaData metadata;
	metadata.file = fopen(path, "rb");
	if (metadata.file != NULL) {
		return Result<TableMetaData, int>::Ok(metadata);
	}
	return Result<TableMetaData, int>::Err(0);
}

bool TableMetaData::create_file(const char* const path) {
	FILE* fp = fopen(path, "wb");
	if (fp != NULL) {
		fclose(fp);
		return true;
	}
	return false;
}

bool TableMetaData::read() {
	if (1 != fread(&this->table, sizeof(TableRec), 1, this->file)) {
		return false;
	}
	ColumnRec new_rec;
	while (1 == fread(&new_rec, sizeof(ColumnRec), 1, this->file)) {
		this->columns.push_back(new_rec);
	}
	return true;
}

bool TableMetaData::write() {
	if (!this->file) {
		// not open
		return false;
	}
	if (1 != fwrite(&this->table, sizeof(TableRec), 1, this->file)) {
		// write failed
		return false;
	}
	for (auto const& column : this->columns) {
		int wrote = fwrite(&column, sizeof(ColumnRec), 1, this->file);
		if (wrote != 1) {
			// write failed
			return false;
		}
	}
	return true;
}

void TableMetaData::close() {
	if (this->file) {
		fclose(file);
		this->file = NULL;
	}
}
