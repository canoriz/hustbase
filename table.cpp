/* Created by Cao, 2021 Nov 14 */
#define _CRT_SECURE_NO_WARNINGS
#include "table.h"
#include "metadata.h"
#include "result.h"

const int PATH_SIZE = 320;

Result<bool, RC> Table::create(char* path, char* name, int sz) {
	RC table = FAIL;
	char full_path[PATH_SIZE] = "";

	// metadata path
	strcpy(full_path, path);
	strcat(full_path, "\\.");
	strcat(full_path, name);
	if (!TableMetaData::create_file(full_path)) {
		// filed create metadata
		return Result<bool, RC>::Err(FAIL);
	}
	table = RM_CreateFile(path, sz);
	if (table == SUCCESS) {
		return Result<bool, RC>(true);
	}
	return Result<bool, RC>::Err(table);
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