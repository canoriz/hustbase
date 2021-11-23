#define _CRT_SECURE_NO_WARNINGS
#include "database.h"

bool DataBase::create(char* dbpath) {
	return (
		Table::create(dbpath, "SYSTABLE", 0, NULL).ok &&
		Table::create(dbpath, "SYSCOLUMN", 0, NULL).ok
		);
}

char* const DataBase::name() {
	return this->sysroot;
}

void DataBase::prefix_root(char* dest, const char* const subdir) {
	// sysroot + "\" + subdir
	strcpy(dest, this->sysroot);
	strcat(dest, "\\");
	strcat(dest, subdir);
}

bool DataBase::in_use() {
	return this->opened;
}

Result<DataBase, RC> DataBase::open(const char* const db_root) {
	DataBase db(db_root);
	char full_path[PATH_SIZE];
	db.prefix_root(full_path, "SYSTABLE");
	FILE* fp = fopen(full_path, "rb");

	if (fp) {
		db.opened = true;
		fclose(fp);
		return Result<DataBase, RC>(db);
	}
	return Result<DataBase, RC>::Err(DB_NOT_EXIST);
}

bool DataBase::close() {
	const bool FORCE_WRITE = true;
	if (this->opened | FORCE_WRITE) {

		bool all_table_closed = true;
		for (auto& t : this->opened_tables) {
			all_table_closed &= t.close();
		}
		this->opened = false;
		return all_table_closed;
	}

	// close nothing???
	return true;
}

bool DataBase::update_table_metadata()
{
	// update table metadata to file
	for (auto& t : this->opened_tables) {
		if (t.dirty) {
			char meta_path[PATH_SIZE] = "";
			this->prefix_root(meta_path, ".");
			strcat(meta_path, t.name);
			t.store_metadata_to(meta_path);
			t.dirty = false;
		}
	}
	this->update_table_metadata_legacy();
	return true;
}

bool DataBase::update_table_metadata_legacy() {
	// legacy mode
	char full_path[PATH_SIZE] = "";
	this->prefix_root(full_path, "SYSTABLE");
	this->drop_table("SYSTABLE");
	this->drop_table("SYSCOLUMN");

	char s01[] = "tablename";
	char s02[] = "attrcount";
	char s11[] = "tablename";
	char s12[] = "attrname";
	char s13[] = "attrtype";
	char s14[] = "attrlength";
	char s15[] = "attroffset";
	char s16[] = "ix_flag";
	char s17[] = "indexname";
	AttrInfo tab[2] = {
		{s01, chars, 20},
		{s02, ints, 4}
	};
	AttrInfo col[7] = {
		{s11, chars, 20},
		{s12, chars, 20},
		{s13, ints, 4},
		{s14, ints, 4},
		{s15, ints, 4},
		{s16, ints, 1},
		{s17, chars, 20}
	};
	Table::create(this->sysroot, "SYSTABLE", 2, tab);
	Table::create(this->sysroot, "SYSCOLUMN", 7, col);
	auto stbl_res = this->open_table("SYSTABLE");
	auto scol_res = this->open_table("SYSCOLUMN");
	if (!stbl_res.ok || !scol_res.ok) {
		return false;
	}
	auto& stbl = stbl_res.result;
	auto& scol = scol_res.result;

	for (auto& t : this->opened_tables) {
		// make [ tablename | attrcount ] char*
		const int SAME = 0;
		if (
			strcmp(t.name, "SYSTABLE") != SAME &&
			strcmp(t.name, "SYSCOLUMN") != SAME
			) {
			int count = t.meta.columns.size();
			char buf[21 + 4];
			strcpy(buf, t.name);
			memcpy(buf + 21, &count, sizeof(int));
			stbl.insert_record(buf);
			for (auto& c : t.meta.columns) {
				/*
				* insert
				* [ tablename | attrname | attrtype | attrlength | attroffset | ix? | ix_name ]
				*/
				scol.insert_record((char*)&c);
			}
		}
	}
	return true;
}

Result<bool, RC> DataBase::remove_file(const char* const file_name) {
	// remove file_name and .file_name
	// remove tables and indices
	char full_path[PATH_SIZE] = "";
	this->prefix_root(full_path, file_name);
	const int OK = 0;

	if (remove(full_path) != OK) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}

	char meta_name[PATH_SIZE] = ".";
	strcat(meta_name, file_name);
	this->prefix_root(full_path, meta_name);
	if (remove(full_path) != OK) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}
	return Result<bool, RC>::Ok(true);
}

bool DataBase::close_table(const char* const table_name) {
	for (auto it = this->opened_tables.begin(); it != this->opened_tables.end(); ++it) {
		const int SAME = 0;
		if (strcmp(table_name, it->name) == SAME) {
			// removed it from opened tables, and there should be no more to remove
			it->close();
			this->opened_tables.erase(it);
			return true;
		}
	}
	return true;
}

bool DataBase::close_index(const char* const index_name) {
	for (auto it = this->opened_indices.begin(); it != this->opened_indices.end(); ++it) {
		const int SAME = 0;
		if (strcmp(index_name, it->name) == SAME) {
			// removed it from opened tables, and there should be no more to remove
			it->close();
			this->opened_indices.erase(it);
			break;
		}
	}
	return true;
}

Result<bool, RC> DataBase::drop_table(char* const table_name) {
	auto open = this->open_table(table_name);
	if (!open.ok) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}
	Table& t = open.result;
	for (auto& c : t.meta.columns) {
		if (c.ix_flag) {
			auto res = this->drop_index(c.indexname);
			if (!res.ok) {
				return Result<bool, RC>::Err(res.err);
			}
		}
	}
	this->close_table(table_name);
	if (strcmp(table_name, "SYSTABLE") != 0 && strcmp(table_name, "SYSCOLUMN") != 0) {
		this->update_table_metadata();
	}
	return this->remove_file(table_name);
}

Result<bool, RC> DataBase::add_table(char* const table_name, int count, AttrInfo* attrs)
{
	auto res = Table::create(this->sysroot, table_name, count, attrs);
	if (!res.ok) {
		return Result<bool, RC>::Err(res.err);
	}
	// open table for update metadata
	this->open_table(table_name);
	this->update_table_metadata();
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> DataBase::add_index(
	char* const table_name,
	char* const column_name,
	char* const index_name) {
	auto t_res = this->open_table(table_name);
	if (!t_res.ok) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}
	auto& t = t_res.result;
	auto c_res = t.get_column(column_name);
	if (!c_res.ok) {
		return Result<bool, RC>::Err(c_res.err);
	}
	auto column_record = c_res.result;

	for (auto& t : this->opened_tables) {
		const int SAME = 0;
		if (strcmp(t.name, table_name) == SAME) {
			if (!t.add_index_flag_on(column_name, index_name)) {
				return Result<bool, RC>::Err(INDEX_EXIST);
			}
			auto res = Index::create(
				this->sysroot, index_name, table_name, column_name,
				(AttrType)column_record->attrtype, column_record->attrlength
			);
			if (!res.ok) {
				// revert index mark
				t.remove_index_flag_on(column_name);
				return Result<bool, RC>::Err(res.err);
			}

			// update table metadata to file
			this->update_table_metadata();

			return Result<bool, RC>::Ok(true);
		}
	}
	return Result<bool, RC>::Err(FAIL);
}

Result<bool, RC> DataBase::drop_index(char* const index_name) {
	auto i_res = this->open_index(index_name);
	if (!i_res.ok) {
		return Result<bool, RC>::Err(INDEX_NOT_EXIST);
	}

	Index& i = i_res.result;
	auto t_res = this->open_table(i.table);
	if (!t_res.ok) {
		// the table that has index is not exist
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}

	for (auto& t : this->opened_tables) {
		const int SAME = 0;
		if (strcmp(t.name, i.table) == SAME) {
			if (!t.remove_index_flag_on(i.column)) {
				return Result<bool, RC>::Err(FAIL);
			}

			this->update_table_metadata();

			if (!this->close_index(index_name)) {
				return Result<bool, RC>::Err(INDEX_NOT_EXIST);
			}
			return this->remove_file(index_name);
		}
	}
	return Result<bool, RC>::Err(INDEX_NOT_EXIST);
}

Result<bool, RC> DataBase::insert(char* const table, const int n, Value* const vals)
{
	// reverse order table(a,b,c,d), values(d,c,b,a)
	auto opent = this->open_table(table);
	if (!opent.ok) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}
	auto& t = opent.result;

	char* buffer = (char*)malloc(21 * n);
	char* here = buffer;
	int processing_column_i = t.meta.columns.size() - 1;
	for (int i = 0; i < t.meta.columns.size(); i++, processing_column_i--) {
		ColumnRec* c_rec = &t.meta.columns[processing_column_i];
		int x = *(int*)vals[i].data;
		char c = *(char*)vals[i].data;
		memcpy(buffer + c_rec->attroffset, vals[i].data, c_rec->attrlength);
	}

	auto ins_res = t.insert_record(buffer);
	if (!ins_res.ok) {
		free(buffer);
		return ins_res.err;
	}
	free(buffer);
	return Result<bool, RC>::Ok(true);
}

Result<Table, RC> DataBase::open_table(char* const table_name) {
	for (auto const& t : this->opened_tables) {
		const int SAME = 0;
		if (strcmp(table_name, t.name) == SAME) {
			return t;
		}
	}
	// not in opened tables
	auto res = Table::open(this->sysroot, table_name);
	if (res.ok) {
		this->opened_tables.push_back(res.result);
		return Result<Table, RC>::Ok(res.result);
	}
	return Result<Table, RC>::Err(res.err);
}

Result<Index, RC> DataBase::open_index(char* const index_name) {
	for (auto const& i : this->opened_indices) {
		const int SAME = 0;
		if (strcmp(index_name, i.name) == SAME) {
			return i;
		}
	}
	// not in opened indices
	auto res = Index::open(this->sysroot, index_name);
	if (res.ok) {
		this->opened_indices.push_back(res.result);
		return Result<Index, RC>::Ok(res.result);
	}
	return Result<Index, RC>::Err(res.err);
}

Result<Table, RC> DataBase::select(
		int n_columns, RelAttr** columns,
		int n_tables, char** tables, int n_conditions,
		Condition* conditions, SelResult* res)
{
	const int SAME = 0;
	bool select_all_columns = false;
	if (n_columns <= 0 || n_tables <= 0 || n_tables > 1) {
		return Result<Table, RC>::Err(FAIL);
	}
	for (auto i = 0; i < n_columns; i++) {
		RelAttr* col_ptr = columns[i];
		if (strcmp(col_ptr->attrName, "*") == SAME) {
			// select * from ???
			select_all_columns = true;
			break;
		}
		else if (strlen(col_ptr->relName) == 0) {
			// table name missing
			for (auto j = 0; j < n_tables; j++) {
				char* table_name = tables[j];

				auto res = this->open_table(table_name);
				if (!res.ok) {
					// can not open such table
					return Result<Table, RC>::Err(TABLE_NOT_EXIST);
				}

				Table tmp_table = res.result;
				if (tmp_table.get_column(col_ptr->relName).ok) {
					// found correspodent table of col_ptr->attrName
					// make up it in RelAttr struct
					strcpy(col_ptr->relName, tmp_table.name);
					break;
				}
			}

			if (strlen(col_ptr->relName) == 0) {
				return Result<Table, RC>::Err(FLIED_NOT_EXIST);
			}
		}
	}
	
	if (select_all_columns) {
		res->row_num = 0;
		res->col_num = 0;
		for (auto j = 0; j < n_tables; j++) {
			// actually n_tables = 1
			char* table_name = tables[j];

			auto open_res = this->open_table(table_name);
			if (!open_res.ok) {
				// can not open such table
				return Result<Table, RC>::Err(TABLE_NOT_EXIST);
			}

			Table tmp_table = open_res.result;
			RM_FileScan file_scan;
			RM_Record rec;
			tmp_table.scan_open(&file_scan, 0, NULL);
			auto scan_res = tmp_table.scan_next(&file_scan, &rec);

			// initiate column titles
			int initiating_col_n = 0;
			for (auto const& c : tmp_table.meta.columns) {
				res->type[initiating_col_n] = (AttrType)c.attrtype;
				res->length[initiating_col_n] = c.attrlength;
				strcpy(res->fields[initiating_col_n], c.attrname);
				initiating_col_n++;
			}

			while (scan_res.ok && scan_res.result) {
				res->col_num = 0;
				res->res[res->row_num] = (char**)malloc(sizeof(char*) * tmp_table.meta.columns.size());
				for (auto const& c : tmp_table.meta.columns) {
					res->res[res->row_num][res->col_num] = (char*)malloc(c.attrlength);
					memcpy(
						res->res[res->row_num][res->col_num],
						rec.pData + c.attroffset, c.attrlength
					);
					res->col_num++;
				}
				res->row_num++;
				scan_res = tmp_table.scan_next(&file_scan, &rec);
			}
		}
		return Result<Table, RC>::Ok(Table());
	}
	return Result<Table, RC>();
}
