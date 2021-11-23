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
		return Result<bool, RC>::Err(TABLE_EXIST);
	}

	auto res = TableMetaData::open(full_path);
	if (!res.ok) {
		// open metadata failed
		return Result<bool, RC>::Err(FAIL);
	}

	// store metadata
	TableMetaData tmeta = res.result;
	int aggregate_size = 0;
	for (auto i = 0; i < count; i++) {
		AttrInfo tmp_attr = attrs[i];
		int real_length = tmp_attr.attrLength;
		if (tmp_attr.attrType == chars) {
			real_length = tmp_attr.attrLength + 1;
		}
		tmeta.columns.push_back(
			ColumnRec(
				name,
				tmp_attr.attrName, tmp_attr.attrType,
				tmp_attr.attrLength, aggregate_size,
				false, ""
			)
		);
		aggregate_size += real_length;
	}

	tmeta.table.attrcount = count;
	strcpy(tmeta.table.tablename, name);
	tmeta.table.size = aggregate_size;
	if (!tmeta.write(full_path)) {
		// write to file failed
		return Result<bool, RC>::Err(FAIL);
	}

	RC table = FAIL;

	strcpy(full_path, path);
	strcat(full_path, "\\");
	strcat(full_path, name);
	table = RM_CreateFile(full_path, aggregate_size);
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
	Result<TableMetaData, int> open = TableMetaData::open(full_path);
	if (!open.ok) {
		return Result<Table, RC>::Err(TABLE_NOT_EXIST);
	}
	auto& tmeta = open.result;
	if (!tmeta.read()) {
		return Result<Table, RC>::Err(FAIL);
	}

	// table path
	strcpy(full_path, path);
	strcat(full_path, "\\");
	strcat(full_path, name);
	Table t;
	t.meta = tmeta;
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

bool Table::remove_index_flag_on(char* const column)
{
	for(auto &c: this->meta.columns) {
		const int SAME = 0;
		if (strcmp(c.attrname, column) == SAME) {
			if (c.ix_flag) {
				c.ix_flag = false;
				this->dirty = true;
				return true;
			}
			else {
				return false;
			}
		}
	}
	return false;
}

bool Table::add_index_flag_on(char* const column, char* const index)
{
	for (auto& c : this->meta.columns) {
		const int SAME = 0;
		if (strcmp(c.attrname, column) == SAME) {
			if (!c.ix_flag) {
				c.ix_flag = true;
				this->dirty = true;
				strcpy(c.indexname, index);
				return true;
			}
			else {
				return false;
			}
		}
	}
	return false;
}

bool Table::store_metadata_to(char* const path)
{
	return this->meta.write(path);
}

Result<RID, RC> Table::insert_record(char* const data)
{
	RID rid;
	RC insert = InsertRec(&this->file, data, &rid);
	if (insert != SUCCESS) {
		return Result<RID, RC>::Err(insert);
	}
	return Result<RID, RC>::Ok(rid);
}

Result<ColumnRec*, RC> Table::get_column(char* const column)
{
	for (auto& c : this->meta.columns) {
		const int SAME = 0;
		if (strcmp(c.attrname, column) == SAME) {
			return Result<ColumnRec*, RC>::Ok(&c);
		}
	}
	return Result<ColumnRec*, RC>::Err(FAIL);
}

Result<bool, RC> Table::scan_open(RM_FileScan* file_scan, int n_con, Con* conditions)
{
	RC scan_opened = OpenScan(file_scan, &this->file, n_con, conditions);
	if (scan_opened != SUCCESS) {
		return Result<bool, RC>::Err(scan_opened);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::scan_next(RM_FileScan* file_scan, RM_Record* rec)
{
	RC next_got = GetNextRec(file_scan, rec);
	if (next_got == RM_EOF) {
		// no next record
		return Result<bool, RC>::Ok(false);
	}
	if (next_got != SUCCESS) {
		return Result<bool, RC>::Err(next_got);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> Table::scan_close(RM_FileScan* file_scan)
{
	RC closed = CloseScan(file_scan);
	if (closed != SUCCESS) {
		return Result<bool, RC>::Err(closed);
	}
	return Result<bool, RC>::Ok(true);
}
