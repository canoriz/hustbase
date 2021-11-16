#define _CRT_SECURE_NO_WARNINGS
#include "index.h"
#include "metadata.h"
#include "result.h"

const int PATH_SIZE = 320;

Result<bool, RC> Index::create(char* path, char* iname, char* tname, char* cname, AttrType atype, int attrlen) {
	// metadata path
	char full_path[PATH_SIZE] = "";
	strcpy(full_path, path);
	strcat(full_path, "\\.");
	strcat(full_path, iname);
	if (!IndexMetaData::create_file(full_path)) {
		// failed create metadata
		return Result<bool, RC>::Err(INDEX_EXIST);
	}

	auto res = IndexMetaData::open(full_path);
	if (!res.ok) {
		// open metadata failed
		return Result<bool, RC>::Err(FAIL);
	}

	// store metadata
	IndexMetaData imeta = res.result;

	strcpy(imeta.index.indexname, iname);
	strcpy(imeta.index.tablename, tname);
	strcpy(imeta.index.columnname, cname);
	if (!imeta.write()) {
		// write to file failed
		return Result<bool, RC>::Err(FAIL);
	}

	RC index = FAIL;

	strcpy(full_path, path);
	strcat(full_path, "\\");
	strcat(full_path, iname);
	index = CreateIndex(full_path, atype, attrlen);
	if (index != SUCCESS) {
		return Result<bool, RC>::Err(index);
	}
	return Result<bool, RC>(true);
}

Result<Index, RC> Index::open(char* path, char* name) {
	char full_path[PATH_SIZE] = "";
	// metadata path
	strcpy(full_path, path);
	strcat(full_path, "\\.");
	strcat(full_path, name);
	Result<IndexMetaData, int> imeta = IndexMetaData::open(full_path);
	if (!imeta.ok) {
		return Result<Index, RC>::Err(INDEX_NOT_EXIST);
	}

	// table path
	strcpy(full_path, path);
	strcat(full_path, "\\");
	strcat(full_path, name);
	Index i;
	i.meta = imeta.result;
	RC res = OpenIndex(full_path, &i.file);
	if (res == SUCCESS) {
		strcpy(i.name, name);
		strcpy(i.table, i.meta.index.tablename);
		strcpy(i.table, i.meta.index.columnname);
		return Result<Index, RC>::Ok(i);
	}
	return Result<Index, RC>::Err(res);
}

bool Index::close() {
	this->meta.close();
	return CloseIndex(&this->file) == SUCCESS;
}