#define _CRT_SECURE_NO_WARNINGS
#include "index.h"
#include "metadata.h"
#include "result.h"

const int PATH_SIZE = 320;

Result<bool, RC> Index::create(char* path, char* iname, char* tname, char* cname, AttrType atype, int attrlen) {
	if (strlen(iname) >= 20) {
		// index name too long
		return Result<bool, RC>::Err(INDEX_NAME_ILLEGAL);
	}
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
	Result<IndexMetaData, int> open = IndexMetaData::open(full_path);
	if (!open.ok) {
		return Result<Index, RC>::Err(INDEX_NOT_EXIST);
	}
	auto& imeta = open.result;
	if (!imeta.read()) {
		return Result<Index, RC>::Err(FAIL);
	}

	// table path
	strcpy(full_path, path);
	strcat(full_path, "\\");
	strcat(full_path, name);
	Index i;
	i.meta = imeta;
	RC res = OpenIndex(full_path, &i.file);
	if (res == SUCCESS) {
		strcpy(i.name, name);
		strcpy(i.table, i.meta.index.tablename);
		strcpy(i.column, i.meta.index.columnname);
		return Result<Index, RC>::Ok(i);
	}
	return Result<Index, RC>::Err(res);
}

Result<bool, RC> Index::insert_entry(void* value, const RID* rid)
{
	RC r = InsertEntry(&this->file, value, rid);
	if (r != SUCCESS) {
		return Result<bool, RC>::Err(r);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Index::delete_entry(void* value, const RID* rid)
{
	RC r = DeleteEntry(&this->file, value, rid);
	if (r != SUCCESS) {
		return Result<bool, RC>::Err(r);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Index::scan_open(IX_IndexScan* idx_scan, CompOp op, char* value)
{
	RC r = OpenIndexScan(idx_scan, &this->file, op, value);
	if (r != SUCCESS) {
		return Result<bool, RC>::Err(r);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Index::scan_next(IX_IndexScan* idx_scan, RID* rid)
{
	RC r = IX_GetNextEntry(idx_scan, rid);
	if (r != SUCCESS) {
		return Result<bool, RC>::Err(r);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Index::scan_close(IX_IndexScan* idx_scan)
{
	RC r = CloseIndexScan(idx_scan);
	if (r != SUCCESS) {
		return Result<bool, RC>::Err(r);
	}
	return Result<bool, RC>::Ok(true);
}

bool Index::close() {
	this->meta.close();
	return CloseIndex(&this->file) == SUCCESS;
}