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
		bool all_index_closed = true;
		for (auto& i : this->opened_indices) {
			all_index_closed &= i.close();
		}
		this->opened = false;
		return all_table_closed & all_index_closed;
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

	if (t.meta.columns.size() < n) {
		return Result<bool, RC>::Err(FIELD_REDUNDAN);
	}
	if (t.meta.columns.size() > n) {
		return Result<bool, RC>::Err(FIELD_MISSING);
	}

	char* buffer = (char*)malloc(21 * n);
	char* here = buffer;
	int processing_column_i = t.meta.columns.size() - 1;
	for (auto i = 0; i < t.meta.columns.size(); i++, processing_column_i--) {
		ColumnRec* c_rec = &t.meta.columns[processing_column_i];
		int x = *(int*)vals[i].data;
		char c = *(char*)vals[i].data;
		if ((AttrType)c_rec->attrtype != vals[i].type) {
			free(buffer);
			return Result<bool, RC>::Err(FIELD_TYPE_MISMATCH);
		}
		memcpy(buffer + c_rec->attroffset, vals[i].data, c_rec->attrlength);
	}

	auto ins_res = t.insert_record(buffer);
	if (!ins_res.ok) {
		free(buffer);
		return ins_res.err;
	}
	for (auto& c : t.meta.columns) {
		if (c.ix_flag) {
			// has index on column c
			// insert index
			auto open_idx = this->open_index(c.indexname);
			if (!open_idx.ok) {
				free(buffer);
				return open_idx.err;
			}
			auto& idx = open_idx.result;
			auto idx_ins_res = idx.insert_entry(
				(void*)(buffer + c.attroffset),
				(const RID*)&ins_res.result
			);
			if (!idx_ins_res.ok) {
				free(buffer);
				return idx_ins_res.err;
			}
		}
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

Result<bool, RC> DataBase::table_product(
		char* const t1,
		char* const t2,
		char* const dest)
{
	/* t1 x t2 -> dest */
	auto r1 = this->open_table(t1);
	auto r2 = this->open_table(t2);
	if (!(r1.ok && r2.ok)) {
		return Result<bool, RC>::Err(TABLE_NOT_EXIST);
	}

	Table& tbl1 = r1.result, tbl2 = r2.result;

	// create table dest
	// first, init paramaters
	int dest_attr_count = tbl1.meta.table.attrcount + tbl2.meta.table.attrcount;
	AttrInfo* dest_attr_arr = (AttrInfo*)malloc(sizeof(AttrInfo) * dest_attr_count);
	int attr_i = 0;
	for (auto const& c : tbl1.meta.columns) {
		dest_attr_arr[attr_i].attrLength = c.attrlength;
		dest_attr_arr[attr_i].attrName = (char*)malloc(
			sizeof(char) * (
				strlen(tbl1.name) + strlen(c.attrname) + 2
				)
		);
		strcpy(dest_attr_arr[attr_i].attrName, c.attrname);
		dest_attr_arr[attr_i].attrType = (AttrType)c.attrtype;
		attr_i++;
	}
	for (auto const& c : tbl2.meta.columns) {
		dest_attr_arr[attr_i].attrLength = c.attrlength;
		dest_attr_arr[attr_i].attrName = (char*)malloc(
			sizeof(char) * (
				strlen(tbl2.name) + strlen(c.attrname) + 2
				)
		);
		strcpy(dest_attr_arr[attr_i].attrName, tbl2.name);
		strcat(dest_attr_arr[attr_i].attrName, ".");
		strcat(dest_attr_arr[attr_i].attrName, c.attrname);
		dest_attr_arr[attr_i].attrType = (AttrType)c.attrtype;
		attr_i++;
	}

	// then, create dest table
	auto add_t = this->add_table(dest, dest_attr_count, dest_attr_arr);
	free(dest_attr_arr);
	if (!add_t.ok) {
		return Result<bool, RC>::Err(add_t.err);
	}
	auto dest_t = this->open_table(dest);
	if (!dest_t.ok) {
		// open destination failed
		return Result<bool, RC>::Err(dest_t.err);
	}

	auto prod_res = tbl1.product(tbl2, dest_t.result);
	if (!prod_res.ok) {
		return Result<bool, RC>::Err(prod_res.err);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> DataBase::delete_record(
		char* const table_name, int n, Condition* conditions
) {
	auto t_open = this->open_table(table_name);
	if (!t_open.ok) {
		// open destination failed
		return Result<bool, RC>::Err(t_open.err);
	}

	auto& t = t_open.result;
	// turn Conditions to Cons
	Con* cons = (Con*)malloc(sizeof(Con) * n);
	for (int i = 0; i < n; i++) {
		auto res = t.turn_to_con(&conditions[i], &cons[i]);
		if (!res.ok) {
			// cannot convert conditions[i] to cons[i]
			free(cons);
			return Result<bool, RC>::Err(res.err);
		}
	}

	RM_FileScan scan;
	RM_Record rec;
	t.scan_open(&scan, n, cons);
	auto scan_res = t.scan_next(&scan, &rec);

	while (scan_res.ok && scan_res.result) {
		for (auto& c : t.meta.columns) {
			if (c.ix_flag) {
				// remove index if index exists
				auto i_open = this->open_index(c.indexname);
				if (!i_open.ok) {
					return Result<bool, RC>::Err(i_open.err);
				}
				auto& i = i_open.result;
				auto delete_result = i.delete_entry(rec.pData + c.attroffset, &rec.rid);
				if (!delete_result.ok) {
					return delete_result;
				}
			}
		}
		auto delete_res = t.remove_by_rid(&rec.rid);
		if (!delete_res.ok) {
			free(cons);
			t.scan_close(&scan);
			return Result<bool, RC>::Err(delete_res.err);
		}
		scan_res = t.scan_next(&scan, &rec);
	}
	t.scan_close(&scan);
	free(cons);
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> DataBase::update_record(
		char* const table_name, char* const column_name,
		Value* v, int n, Condition* conditions
) {
	auto t_open = this->open_table(table_name);
	if (!t_open.ok) {
		// open destination failed
		return Result<bool, RC>::Err(t_open.err);
	}
	auto& t = t_open.result;

	//auto update_res = t.update_match(column_name, v, n, conditions);
	// turn Conditions to Cons
	Con* cons = (Con*)malloc(sizeof(Con) * n);
	for (int i = 0; i < n; i++) {
		auto res = t.turn_to_con(&conditions[i], &cons[i]);
		if (!res.ok) {
			// cannot convert conditions[i] to cons[i]
			free(cons);
			return Result<bool, RC>::Err(res.err);
		}
	}

	RM_FileScan scan;
	RM_Record rec;
	t.scan_open(&scan, n, cons);
	auto scan_res = t.scan_next(&scan, &rec);

	while (scan_res.ok && scan_res.result) {
		auto find_column = t.get_column(column_name);
		if (!find_column.ok) {
			free(cons);
			t.scan_close(&scan);
			return Result<bool, RC>::Err(find_column.err);
		}
		auto const& col = find_column.result;
		if (v->type != col->attrtype) {
			free(cons);
			t.scan_close(&scan);
			return Result<bool, RC>::Err(TYPE_NOT_MATCH);
		}
		if (col->ix_flag) {
			// update index
			auto res = this->open_index(col->indexname);
			if (!res.ok) {
				free(cons);
				t.scan_close(&scan);
				return Result<bool, RC>::Err(res.err);
			}
			auto& i = res.result;
			auto delete_result = i.delete_entry(rec.pData + col->attroffset, &rec.rid);
			if (!delete_result.ok) {
				free(cons);
				t.scan_close(&scan);
				return delete_result;
			}
			memcpy(rec.pData + col->attroffset, v->data, col->attrlength);
			auto insert_result = i.insert_entry(rec.pData + col->attroffset, &rec.rid);
			if (!insert_result.ok) {
				free(cons);
				t.scan_close(&scan);
				return insert_result;
			}
		}
		memcpy(rec.pData + col->attroffset, v->data, col->attrlength);
		RC update_res = UpdateRec(&t.file, &rec);
		if (update_res != SUCCESS) {
			free(cons);
			t.scan_close(&scan);
			return Result<bool, RC>::Err(update_res);
		}
		scan_res = t.scan_next(&scan, &rec);
	}
	t.scan_close(&scan);
	free(cons);
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> DataBase::table_project(
	char* const t, char* const dest,
	int n, RelAttr** columns)
{
	auto r_from = this->open_table(t);
	if (!r_from.ok) {
		return Result<bool, RC>::Err(r_from.err);
	}
	Table& tbl = r_from.result;

	// create table dest
	// first, init paramaters
	// match every column
	AttrInfo* dest_attr_arr = (AttrInfo*)malloc(sizeof(AttrInfo) * tbl.meta.table.attrcount);
	int attr_i = 0;
	for (auto j = 0; j < n; j++) {
		char col_with_dot[PATH_SIZE];
		strcpy(col_with_dot, columns[j]->relName);
		strcat(col_with_dot, ".");
		strcat(col_with_dot, columns[j]->attrName);

		bool match = false;
		for (auto const& c : tbl.meta.columns) {
			const int SAME = 0;
			if (strcmp(col_with_dot, c.attrname) == SAME) {
				dest_attr_arr[attr_i].attrLength = c.attrlength;
				dest_attr_arr[attr_i].attrName = (char*)malloc(
					sizeof(char) * (strlen(c.attrname) + 2)
				);
				strcpy(dest_attr_arr[attr_i].attrName, c.attrname);
				dest_attr_arr[attr_i].attrType = (AttrType)c.attrtype;
				attr_i++;
				match = true;
			}
		}
		if (!match) {
			return Result<bool, RC>::Err(FAIL);
		}
	}

	// then, create dest table
	auto add_t = this->add_table(dest, attr_i, dest_attr_arr);
	free(dest_attr_arr);
	if (!add_t.ok) {
		return Result<bool, RC>::Err(add_t.err);
	}
	auto dest_t = this->open_table(dest);
	if (!dest_t.ok) {
		// open destination failed
		return Result<bool, RC>::Err(dest_t.err);
	}

	auto proj_res = tbl.project(dest_t.result);
	if (!proj_res.ok) {
		return Result<bool, RC>::Err(proj_res.err);
	}
	return Result<bool, RC>::Ok(true);
}

Result<bool, RC> DataBase::table_select(
		char* const t, char* const dest,
		int n, Condition* conditions
)
{
	auto r_from = this->open_table(t);
	if (!r_from.ok) {
		return Result<bool, RC>::Err(r_from.err);
	}
	Table& tbl = r_from.result;

	// create table dest
	// first, init paramaters
	AttrInfo* dest_attr_arr = (AttrInfo*)malloc(sizeof(AttrInfo) * tbl.meta.table.attrcount);
	int attr_i = 0;
	for (auto const& c : tbl.meta.columns) {
		dest_attr_arr[attr_i].attrLength = c.attrlength;
		dest_attr_arr[attr_i].attrName = (char*)malloc(
			sizeof(char) * strlen(tbl.name)
		);
		strcpy(dest_attr_arr[attr_i].attrName, c.attrname);
		dest_attr_arr[attr_i].attrType = (AttrType)c.attrtype;
		attr_i++;
	}

	// then, create dest table
	auto add_t = this->add_table(dest, attr_i, dest_attr_arr);
	free(dest_attr_arr);
	if (!add_t.ok) {
		return Result<bool, RC>::Err(add_t.err);
	}
	auto dest_t = this->open_table(dest);
	if (!dest_t.ok) {
		// open destination failed
		return Result<bool, RC>::Err(dest_t.err);
	}

	return tbl.select(dest_t.result, n, conditions);
}

Result<bool, RC> DataBase::make_unit_table(char* const table_name)
{
	auto mk_unit = this->add_table(table_name, 0, NULL);
	if (!mk_unit.ok) {
		return Result<bool, RC>::Err(mk_unit.err);
	}
	auto unit_t_open = this->open_table(table_name);
	if (!unit_t_open.ok) {
		return Result<bool, RC>::Err(unit_t_open.err);
	}
	auto& unit_t = unit_t_open.result;
	auto insert_none = unit_t.insert_record("");
	if (!insert_none.ok) {
		return Result<bool, RC>::Err(insert_none.err);
	}
	return Result<bool, RC>::Ok(true);
}

Result<Table, RC> DataBase::query(
		int n_columns, RelAttr** columns,
		int n_tables, char** tables, int n_conditions,
		Condition* conditions, SelResult* res)
{
	const int SAME = 0;
	bool query_all_columns = false;
	if (n_columns <= 0 || n_tables <= 0) {
		return Result<Table, RC>::Err(FAIL);
	}

	// make columns
	for (auto i = 0; i < n_columns; i++) {
		RelAttr* col_ptr = columns[i];
		if (strcmp(col_ptr->attrName, "*") == SAME) {
			// select * from ???
			query_all_columns = true;
			break;
		}
		else if (col_ptr->relName == NULL) {
			// table name missing, get it!
			for (auto j = 0; j < n_tables; j++) {
				char* table_name = tables[j];

				auto res = this->open_table(table_name);
				if (!res.ok) {
					// can not open such table
					return Result<Table, RC>::Err(TABLE_NOT_EXIST);
				}

				Table tmp_table = res.result;
				if (tmp_table.get_column(col_ptr->attrName).ok) {
					// found correspodent table of col_ptr->attrName
					// make up it in RelAttr struct
					col_ptr->relName = (char*)malloc(
						sizeof(char) * (1 + strlen(tmp_table.name))
					);
					strcpy(col_ptr->relName, tmp_table.name);
					break;
				}
			}

			if (col_ptr->relName == NULL) {
				// FLIED£¿ FIELD£¡
				return Result<Table, RC>::Err(FLIED_NOT_EXIST);
			}
		}
	}

	for (auto i = 0; i < n_conditions; i++) {
		Condition* cond_ptr = &conditions[i];
		if (cond_ptr->bLhsIsAttr && cond_ptr->lhsAttr.relName == NULL) {
			// table name missing, get it!
			for (auto j = 0; j < n_tables; j++) {
				char* table_name = tables[j];

				auto res = this->open_table(table_name);
				if (!res.ok) {
					// can not open such table
					return Result<Table, RC>::Err(TABLE_NOT_EXIST);
				}

				Table tmp_table = res.result;
				if (tmp_table.get_column(cond_ptr->lhsAttr.attrName).ok) {
					// found correspodent table of col_ptr->attrName
					// make up it in RelAttr struct
					cond_ptr->lhsAttr.relName = (char*)malloc(
						sizeof(char) * (1 + strlen(tmp_table.name))
					);
					strcpy(cond_ptr->lhsAttr.relName, tmp_table.name);
					break;
				}
			}

			if (cond_ptr->lhsAttr.relName == NULL) {
				// FLIED£¿ FIELD£¡
				return Result<Table, RC>::Err(FLIED_NOT_EXIST);
			}
		}
		if (cond_ptr->bRhsIsAttr && cond_ptr->rhsAttr.relName == NULL) {
			// table name missing, get it!
			for (auto j = 0; j < n_tables; j++) {
				char* table_name = tables[j];

				auto res = this->open_table(table_name);
				if (!res.ok) {
					// can not open such table
					return Result<Table, RC>::Err(TABLE_NOT_EXIST);
				}

				Table tmp_table = res.result;
				if (tmp_table.get_column(cond_ptr->rhsAttr.attrName).ok) {
					// found correspodent table of col_ptr->attrName
					// make up it in RelAttr struct
					cond_ptr->rhsAttr.relName = (char*)malloc(
						sizeof(char) * (1 + strlen(tmp_table.name))
					);
					strcpy(cond_ptr->rhsAttr.relName, tmp_table.name);
					break;
				}
			}

			if (cond_ptr->rhsAttr.relName == NULL) {
				// FLIED£¿ FIELD£¡
				return Result<Table, RC>::Err(FLIED_NOT_EXIST);
			}
		}
	}

	if (
			n_tables == 1 &&
			n_conditions == 1 &&
			query_all_columns &&
			conditions[0].bLhsIsAttr &&
			!conditions[0].bRhsIsAttr
		) {
		char* the_only_table = tables[0];
		auto t_open = this->open_table(the_only_table);
		if (!t_open.ok) {
			return t_open;
		}
		auto& t = t_open.result;
		char* the_only_column = conditions[0].lhsAttr.attrName;
		auto column_meta_res = t.get_column(the_only_column);
		if (!column_meta_res.ok) {
			return Result<Table, RC>::Err(column_meta_res.err);
		}
		auto& c = column_meta_res.result;
		if (c->ix_flag) {
			// has index, use index
			char* dest = this->get_a_tmp_table();
			AttrInfo* attrs = (AttrInfo*)malloc(sizeof(AttrInfo) * t.meta.columns.size());
			int attr_count = 0;
			for (const auto& c : t.meta.columns) {
				attrs[attr_count].attrLength = c.attrlength;
				attrs[attr_count].attrName = (char*)c.attrname;
				attrs[attr_count].attrType = (AttrType)c.attrtype;
				attr_count++;
			}
			auto create_dest = this->add_table(dest, t.meta.columns.size(), attrs);
			free(attrs);
			if (!create_dest.ok) {
				return Result<Table, RC>::Err(create_dest.err);
			}
			auto dest_open = this->open_table(dest);
			auto& dest_table = dest_open.result;
			if (!dest_open.ok) {
				this->drop_table(dest);
				return Result<Table, RC>::Err(FAIL);
			}

			auto i_open = this->open_index(c->indexname);
			if (!i_open.ok) {
				return Result<Table, RC>::Err(i_open.err);
			}
			auto& i = i_open.result;
			IX_IndexScan idx_scan;
			RID rid;
			auto scan_open_res = i.scan_open(
				&idx_scan,
				conditions[0].op,
				(char*)conditions[0].rhsValue.data
			);
			if (!scan_open_res.ok) {
				return Result<Table, RC>::Err(scan_open_res.err);
			}

			auto scan_next_res = i.scan_next(&idx_scan, &rid);
			while (scan_next_res.ok) {
				RM_Record rec;
				auto get_rec_res = t.get_by_rid(&rid, &rec);
				if (!get_rec_res.ok) {
					return Result<Table, RC>::Err(get_rec_res.err);
				}
				auto insert_res = dest_table.insert_record(rec.pData);
				if (!insert_res.ok) {
					return Result<Table, RC>::Err(insert_res.err);
				}
				scan_next_res = i.scan_next(&idx_scan, &rid);
			}
			i.scan_close(&idx_scan);
			dest_table.make_select_result(res);
			// TODO: make table
			this->release_all_tmp_tables();
			return Result<Table, RC>::Ok(Table());
		}
	}


	/* unit table reserved-unit:
	   for any t,
	   cartesian_product(u, t) = t
	   cartesian_product(t, u) = t */
	char* t1 = NULL;
	char* t2 = NULL;
	char* dest = this->get_a_tmp_table();
	if (!this->make_unit_table(dest).ok ||
		!this->open_table(dest).ok) {
		this->drop_table(dest);
		return Result<Table, RC>::Err(FAIL);
	}

	// make product
	for (auto i = 0; i < n_tables; i++) {
		t1 = dest;
		t2 = tables[i];
		dest = get_a_tmp_table();
		auto mk_prod = this->table_product(t1, t2, dest);
		if (!mk_prod.ok) {
			this->release_all_tmp_tables();
			return Result<Table, RC>::Err(mk_prod.err);
		}
	}

	t1 = dest;
	dest = get_a_tmp_table();
	auto sele_res = this->table_select(t1, dest, n_conditions, conditions);
	if (!sele_res.ok) {
		this->release_all_tmp_tables();
		return Result<Table, RC>::Err(sele_res.err);
	}
	
	if (!query_all_columns) {
		// get specific columns
		t1 = dest;
		dest = this->get_a_tmp_table();

		auto proj_res = this->table_project(t1, dest, n_columns, columns);
		if (!proj_res.ok) {
			this->release_all_tmp_tables();
			return Result<Table, RC>::Err(proj_res.err);
		}
	}

	auto open_res = this->open_table(dest);
	if (!open_res.ok) {
		// can not open such table
		this->release_all_tmp_tables();
		return Result<Table, RC>::Err(TABLE_NOT_EXIST);
	}

	Table tmp_table = open_res.result;
	tmp_table.make_select_result(res);
	// TODO: make table
	this->release_all_tmp_tables();
	return Result<Table, RC>::Ok(Table());
}

char* const DataBase::get_a_tmp_table()
{
	// maximum 16 predefined middle results
	static const char tmp_tables[16][16] = {
			"reserved-tmp0",
			"reserved-tmp1",
			"reserved-tmp2",
			"reserved-tmp3",
			"reserved-tmp4",
			"reserved-tmp5",
			"reserved-tmp6",
			"reserved-tmp7",

			"reserved-tmp8",
			"reserved-tmp9",
			"reserved-tmpa",
			"reserved-tmpb",
			"reserved-tmpc",
			"reserved-tmpd",
			"reserved-tmpe",
			"reserved-tmpf"
	};
	char* const out_table = (char*)tmp_tables[this->next_tmp_table];
	this->next_tmp_table++;
	return out_table;
}

bool DataBase::release_all_tmp_tables()
{
	// maximum 16 predefined middle results
	static const char tmp_tables[16][16] = {
			"reserved-tmp0",
			"reserved-tmp1",
			"reserved-tmp2",
			"reserved-tmp3",
			"reserved-tmp4",
			"reserved-tmp5",
			"reserved-tmp6",
			"reserved-tmp7",

			"reserved-tmp8",
			"reserved-tmp9",
			"reserved-tmpa",
			"reserved-tmpb",
			"reserved-tmpc",
			"reserved-tmpd",
			"reserved-tmpe",
			"reserved-tmpf"
	};
	bool all_released = true;
	for (int i = 0; i < this->next_tmp_table; i++) {
		all_released = all_released & this->drop_table((char*)tmp_tables[i]).ok;
	}
	this->next_tmp_table = 0;
	return all_released;
}
