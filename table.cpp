/* Created by Cao, 2021 Nov 14 */
#define _CRT_SECURE_NO_WARNINGS
#include "table.h"
#include "metadata.h"
#include "result.h"

const int PATH_SIZE = 320;

Result<bool, RC> Table::create(char* path, char* name, int count, AttrInfo* attrs) {
	// metadata path
	char full_path[PATH_SIZE] = "";
	strcpy(full_path, path);
	strcat(full_path, "\\.");
	strcat(full_path, name);
	if (!TableMetaData::create_file(full_path)) {
		// failed create metadata
		return Result<bool, RC>::Err(FAIL);
	}

	auto res = TableMetaData::open(full_path);
	if (!res.ok) {
		// open metadata failed
		return Result<bool, RC>::Err(FAIL);
	}

	// store metadata
	TableMetaData tmeta = res.result;
	tmeta.table.attrcount = count;
	strcpy(tmeta.table.tablename, name);
	int aggregate_size = 0;
	for (auto i = 0; i < count; i++) {
		AttrInfo tmp_attr = attrs[i];
		tmeta.columns.push_back(
			ColumnRec(
				tmp_attr.attrName, tmp_attr.attrType,
				tmp_attr.attrLength, aggregate_size,
				false, ""
			)
		);
		aggregate_size += tmp_attr.attrLength;
	}
	if (!tmeta.write()) {
		// write to file failed
		return Result<bool, RC>::Err(FAIL);
	}

	RC table = FAIL;

	table = RM_CreateFile(path, aggregate_size);
	if (table != SUCCESS) {
		return Result<bool, RC>::Err(table);
	}
	return Result<bool, RC>(true);
}

Result<Table, RC> Table::open(char* path, char* name) {
	char full_path[PATH_SIZE] = "";
	// metadata path
	strcpy(full_path, path);
	strcat(full_path, "\\.");
	strcat(full_path, name);
	Result<TableMetaData, int> tmeta = TableMetaData::open(full_path);
	if (!tmeta.ok) {
		return Result<Table, RC>::Err(TABLE_NOT_EXIST);
	}

	// table path
	strcpy(full_path, path);
	strcat(full_path, "\\");
	strcat(full_path, name);
	Table t;
	t.meta = tmeta.result;
	RC res = RM_OpenFile(full_path, &t.file);
	if (res == SUCCESS) {
		strcpy(t.name, name);
		return Result<Table, RC>::Ok(t);
	}
	return Result<Table, RC>::Err(res);
}

bool Table::close() {
	this->meta.close();
	return RM_CloseFile(&this->file) == SUCCESS;
}

bool Table::destroy() {
	//TODO
	return true;
}
