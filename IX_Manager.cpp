#include "IX_Manager.h"

#define PTR_SIZE sizeof(RID)

struct IndexInfo{
	AttrType attrType;
	int attrLength;
	IndexInfo(IX_IndexHandle* indexHandle): attrType(indexHandle->fileHeader.attrType), attrLength(indexHandle->fileHeader.attrLength) {
	}
};

//����attryypeָ�������ͱȽ����������ֵ
int CmpValue(AttrType attrType, char* keyval, char* value);

//����B+��������ֵ������value��Ҷ�ӽڵ㣬��������������ҳ���ҳ������ҳ���
RC FindFirstIXNode(IX_IndexHandle* indexHandle, char* value, PF_PageHandle* pageHandle);

//��������ֵ��RID��ֵ��ȷ��������������ڵ�ҳ�棬����ҳ�����;���λ��
RC FindIXNode(IX_IndexHandle* indexHandle, char* keyval, const RID* ridval, PF_PageHandle* pageHandle, int* ridIx);

//��������ֵ��ҳ�Ų������ݵ������ҳ����
RC InsertEntryInPage(IX_IndexHandle* indexHandle, void* pData, const RID* rid, PageNum pn);



//����pageHandle��ȡҳ���е�IX_Node�ڵ���Ϣ
IX_Node* getIXNodefromPH(const PF_PageHandle* pageHandle) {
	return (IX_Node*)(pageHandle->pFrame->page.pData + sizeof(IX_FileHeader));
}


RC CreateIndex(const char* fileName, AttrType attrType, int attrLength)
{
	//������Ӧ��ҳ���ļ�
	RC tmp;
	if ((tmp = CreateFile(fileName)) != SUCCESS) {
		return tmp;
	}

	//�򿪴������ļ�
	int fileID;
	if ((tmp = OpenFile((char*)fileName, &fileID)) != SUCCESS) {
		return tmp;
	}

	//��ȡ��һҳ����������������Ϣ
	PF_PageHandle pageHandle;
	if ((tmp = AllocatePage(fileID, &pageHandle)) != SUCCESS) {
		return tmp;
	}

	IX_FileHeader* fileHeader = (IX_FileHeader*)pageHandle.pFrame->page.pData;
	fileHeader->attrLength = attrLength;
	fileHeader->attrType = attrType;
	fileHeader->rootPage = 1;
	fileHeader->first_leaf = 1;
	fileHeader->order = (PF_PAGE_SIZE - sizeof(IX_FileHeader) - sizeof(IX_Node) - PTR_SIZE) / (PTR_SIZE + attrLength);

	IX_Node* IXNode = getIXNodefromPH(&pageHandle);
	IXNode->is_leaf = 1;
	IXNode->keynum = 0;
	IXNode->parent = 0;
	IXNode->brother = 0;
	IXNode->keys = pageHandle.pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
	IXNode->rids = (RID*)(IXNode->keys + fileHeader->order * attrLength);

	//�޸�ҳ������PF�к������д���
	MarkDirty(&pageHandle);

	UnpinPage(&pageHandle);

	if ((tmp = CloseFile(fileID)) != SUCCESS) {
		return tmp;
	}

	return SUCCESS;
}

RC OpenIndex(const char* fileName, IX_IndexHandle* indexHandle)
{
	if (indexHandle->bOpen == true) {
		return IX_IHOPENNED;
	}

	//���ļ�
	RC tmp;
	int fileID;
	if ((tmp = OpenFile((char*)fileName, &fileID)) != SUCCESS) {
		return tmp;
	}

	//��ȡ����������Ϣ
	PF_PageHandle* pageHandle = (PF_PageHandle*)malloc(sizeof(PF_PageHandle));
	if ((tmp = GetThisPage(fileID, 0, pageHandle)) != SUCCESS) {
		return tmp;
	}

	indexHandle->bOpen = true;
	indexHandle->fileID = fileID;
	indexHandle->fileHeader = *(IX_FileHeader*)pageHandle->pFrame->page.pData;

	UnpinPage(pageHandle);

	free(pageHandle);

	return RC();
}

RC CloseIndex(IX_IndexHandle* indexHandle)
{
	if (indexHandle->bOpen == false) {
		return IX_IHCLOSED;
	}

	indexHandle->bOpen = false;
	RC tmp;
	if ((tmp = CloseFile(indexHandle->fileID)) != SUCCESS) {
		return tmp;
	}

	return SUCCESS;
}

RC InsertEntry(IX_IndexHandle* indexHandle, void* pData, const RID* rid)
{
	if (indexHandle->bOpen == false) {
		return IX_IHCLOSED;
	}

	RC tmp;
	IndexInfo indexinfo(indexHandle);
	int order = indexHandle->fileHeader.order;
	PF_PageHandle pageHandle;

	FindFirstIXNode(indexHandle, (char*)pData, &pageHandle);

	int pn = pageHandle.pFrame->page.pageNum;

	UnpinPage(&pageHandle);

	InsertEntryInPage(indexHandle, pData, rid, pn);
	

	return SUCCESS;
}

RC DeleteEntry(IX_IndexHandle* indexHandle, void* pData, const RID* rid)
{
	if (indexHandle->bOpen == false) {
		return IX_IHCLOSED;
	}

	RC tmp;
	IndexInfo indexinfo(indexHandle);
	int order = indexHandle->fileHeader.order;
	PF_PageHandle pageHandle;
	int ridIx;

	tmp = FindIXNode(indexHandle, (char*)pData, rid, &pageHandle, &ridIx);
	if (tmp != SUCCESS) {
		return IX_NOMEM;
	}
	
	return SUCCESS;
}

RC OpenIndexScan(IX_IndexScan* indexScan, IX_IndexHandle* indexHandle, CompOp compOp, char* value)
{
	if (indexScan->bOpen == true) {
		return IX_SCANOPENNED;
	}

	RC tmp;
	PF_PageHandle pageHandle;
	PageNum pn;
	
	tmp = FindFirstIXNode(indexHandle, value, &pageHandle);
	if (tmp != SUCCESS) return tmp;

	IX_Node* ixNode = getIXNodefromPH(&pageHandle);
	pn = ixNode->brother;
	IndexInfo indexinfo(indexHandle);
	char* keyval = (char*)malloc(indexinfo.attrLength);

	int ridIx = 0;
	switch (indexScan->compOp)
	{
	case EQual:
		break;
	case LEqual:
		pn = indexHandle->fileHeader.first_leaf;
		break;
	case NEqual:
		return FAIL;
		break;
	case LessT:
		pn = indexHandle->fileHeader.first_leaf;
		break;
	case GEqual:
		while (ridIx < ixNode->keynum) {
			memcpy(keyval, ixNode->keys + ridIx * indexinfo.attrLength, indexinfo.attrLength);
			int result = CmpValue(indexinfo.attrType, keyval, value);
			if (result < 0) {
				ridIx++;
			}
			else {
				break;
			}
		}
		break;
	case GreatT:
		while (ridIx < ixNode->keynum) {
			//�����ȡ�����һ��Ҷ�ӽڵ�����һ��������ͷ���EOF
			if (ridIx == ixNode->keynum && ixNode->brother == 0) {
				return FAIL;
			}

			//��������ýڵ����һ����������л�����һҳ��
			if (ridIx == ixNode->keynum) {
				UnpinPage(&pageHandle);
				if ((tmp = GetThisPage(indexHandle->fileID, ixNode->brother, &pageHandle)) != SUCCESS) {
					return tmp;
				}
				pn = ixNode->brother;
				ixNode = getIXNodefromPH(&pageHandle);
				ridIx = 0;
			}
			memcpy(keyval, ixNode->keys + ridIx * indexinfo.attrLength, indexinfo.attrLength);
			int result = CmpValue(indexinfo.attrType, keyval, value);
			if (result <= 0) {
				ridIx++;
			}
			else {
				break;
			}
		}
		break;
	case NO_OP:
		pn = indexHandle->fileHeader.first_leaf;
		break;
	default:
		break;
	}

	indexScan->bOpen = true;
	indexScan->pIXIndexHandle = indexHandle;
	indexScan->compOp = compOp;
	indexScan->value = value;
	indexScan->PageHandle = pageHandle;
	indexScan->pn = pn;
	indexScan->ridIx = ridIx;

	return SUCCESS;
}

RC IX_GetNextEntry(IX_IndexScan* indexScan, RID* rid)
{
	if (indexScan->bOpen == false) {
		return IX_ISCLOSED;
	}

	//��PH�ڴ��л�ȡIX_Node��Ϣ
	IX_Node* ixNode = getIXNodefromPH(&indexScan->PageHandle);
	RC tmp;

	//�����ȡ�����һ��Ҷ�ӽڵ�����һ��������ͷ���EOF
	if (indexScan->ridIx == ixNode->keynum && indexScan->pn == 0) {
		return IX_EOF;
	}

	//��������ýڵ����һ����������л�����һҳ��
	if (indexScan->ridIx == ixNode->keynum) {
		UnpinPage(&indexScan->PageHandle);
		if ((tmp = GetThisPage(indexScan->pIXIndexHandle->fileID, indexScan->pn, &indexScan->PageHandle)) != SUCCESS) {
			return tmp;
		}
		indexScan->pn = ixNode->brother;
		indexScan->ridIx = 0;
	}

	//��ȡ����ֵ�������Ϣ
	IndexInfo indexinfo(indexScan->pIXIndexHandle);
	char*  keyval = (char*)malloc(indexinfo.attrLength);
	memcpy(keyval, ixNode->keys + indexScan->ridIx * indexinfo.attrLength, indexinfo.attrLength);
	int result = CmpValue(indexinfo.attrType, keyval, indexScan->value);
	free(keyval);
	//���ݲ������Խ�����д���
	switch (indexScan->compOp)
	{
	case EQual:
		if (result != 0) {
			return IX_NOMOREIDXINMEM;
		}
		break;
	case LEqual:
		if (result > 0) {
			return IX_NOMOREIDXINMEM;
		}
		break;
	case NEqual:
		return FAIL;
		break;
	case LessT:
		if (result >= 0) {
			return IX_NOMOREIDXINMEM;
		}
		break;
	case GEqual:
		break;
	case GreatT:
		break;
	case NO_OP:
		break;
	default:
		break;
	}

	*rid = *(ixNode->rids + indexScan->ridIx);
	indexScan->ridIx++;

	return SUCCESS;
}

RC CloseIndexScan(IX_IndexScan* indexScan)
{
	if (indexScan->bOpen == false) {
		return IX_SCANCLOSED;
	}

	RC tmp;
	indexScan->bOpen = false;

	UnpinPage(&indexScan->PageHandle);

	return SUCCESS;
}

RC GetIndexTree(char* fileName, Tree* index) {
	return RC();
};

int CmpValue(AttrType attrType, char* keyval, char* value) {
	int result;
	float fres = *(float*)keyval - *(float*)value;
	//�����������ͻ�ȡ�ȽϽ��
	switch (attrType)
	{
	case ints:
		result = *(int*)keyval - *(int*)value;
		break;
	case chars:
		result = strcmp(keyval,value);
		break;
	case floats:
		if (fres < 0) {
			result = -1;
		}
		else if (fres > 0) {
			result = 1;
		}
		else {
			result = 0;
		}
		break;
	default:
		result = strcmp(keyval, value);
		break;
	}

	return result;
}


RC FindFirstIXNode(IX_IndexHandle* indexHandle, char* value, PF_PageHandle* pageHandle) {
	//�Ӹ��ڵ㿪ʼ������Ҫ��ֵ
	PageNum _pn = indexHandle->fileHeader.rootPage;

	IX_Node* ixNode;
	IndexInfo indexinfo(indexHandle);
	char* _keyval = (char*)malloc(indexinfo.attrLength);

	PF_PageHandle _pageHandle;
	RC tmp;
	int _ridIx;
	while(1){

		//ÿ�ζ�ȡҳ��Ϊ_pn��ҳ����д���
		if ((tmp = GetThisPage(indexHandle->fileID, _pn, &_pageHandle)) != SUCCESS) {
			free(_keyval);
			return tmp;
		}

		//��ȡ�ڵ���Ϣ�������Ҷ�ӽڵ��ֱ�ӷ���
		ixNode = (IX_Node*)(_pageHandle.pFrame->page.pData + sizeof(IX_FileHeader));
		if (ixNode->is_leaf == 1) {
			break;
		}

		//��ȡ�ڵ������ֵ������ĵ�ֵ���бȽϣ�����ֵС�ڸ���ֵ������ƶ�
		int result = -1;
		for (_ridIx = 0; _ridIx < ixNode->keynum; _ridIx++) {
			memcpy(_keyval, ixNode->keys + _ridIx * indexinfo.attrLength, indexinfo.attrLength);
			result = CmpValue(indexinfo.attrType, _keyval, value);
			if (result >= 0) {
				break;
			}
		}
		_pn = *(PageNum*)(ixNode->rids + _ridIx);
		UnpinPage(&_pageHandle);
	} 
	*pageHandle = _pageHandle;
	free(_keyval);
	return SUCCESS;
}

RC FindIXNodeInRid(IX_IndexHandle* indexHandle, char* keyval, const RID* ridval, PF_PageHandle* pageHandle, int* ridIx) {
	//���ȵ��ò���RID�����жϵ�FindIXNode��keyval���з�Χ�ų�
	RC tmp;
	PF_PageHandle _pageHandle;
	PageNum _pn;
	if ((tmp = FindFirstIXNode(indexHandle, keyval, &_pageHandle)) != SUCCESS) {
		return tmp;
	}
	
	//ͨ��indexHandle��ȡ�����������Ϣ
	IX_Node* ixNode;
	IndexInfo indexinfo(indexHandle);
	char* _keyval = (char*)malloc(indexinfo.attrLength);
	RID _ridval;
	
	int _ridIx = 0;
	int success = 0, fail = 0;//��������ѭ��
	while (!success && !fail) {
		//��ȡ�ڵ���Ϣ
		ixNode = getIXNodefromPH(&_pageHandle);

		//��ȡ�ڵ������ֵ������ĵ�ֵ���бȽϣ�����ֵС�ڸ���ֵ������ƶ�
		for (_ridIx = 0; _ridIx < ixNode->keynum && success && fail; _ridIx++) {
			memcpy(_keyval, ixNode->keys + _ridIx * indexinfo.attrLength, indexinfo.attrLength);
			int result1 = CmpValue(indexinfo.attrType, _keyval, keyval);
			if (result1 == 0) {
				memcpy(&_ridval, ixNode->rids + _ridIx, indexinfo.attrLength);
				if (memcmp(&_ridval, ridval, indexinfo.attrLength) == 0) {
					*pageHandle = _pageHandle;
					*ridIx = _ridIx;
					success = 1;
				}
			}
			else if (result1 < 0) {
				continue;
			}
			else {
				fail = 1;
			}
		}

		//�������Ҳû�ҵ����л���һ��Ҷ�ӽڵ�
		_pn = ixNode->brother;
		_ridIx = 0;
		UnpinPage(&_pageHandle);

		//�����ȡ�����һ��Ҷ�ӽڵ��ֱ�ӷ���
		if (_pn == 0) {
			fail = 1;
		}

		if ((tmp = GetThisPage(indexHandle->fileID, _pn, &_pageHandle)) != SUCCESS) {
			free(_keyval);
			return tmp;
		}
	}
	
	free(_keyval);
	if (fail) {
		return FAIL;
	}
	*pageHandle = _pageHandle;
	*ridIx = _ridIx;
	return SUCCESS;
}


RC InsertEntryInPage(IX_IndexHandle* indexHandle, void* pData, const RID* rid, PageNum pn) {

	RC tmp;
	IndexInfo indexinfo(indexHandle);
	int order = indexHandle->fileHeader.order;
	PF_PageHandle pageHandle;

	if ((tmp = GetThisPage(indexHandle->fileID, pn, &pageHandle)) != SUCCESS) {
		return tmp;
	}

	IX_Node* ixNode = getIXNodefromPH(&pageHandle);

	char* keyval = (char*)malloc(indexinfo.attrLength);
	int i;
	for (i = 0; i < ixNode->keynum; i++) {
		memcpy(keyval, ixNode->keys + i * indexinfo.attrLength, indexinfo.attrLength);
		int result = CmpValue(indexinfo.attrType, keyval, (char*)pData);
		if (result >= 0) {
			break;
		}
	}
	int _index = i;
	free(keyval);

	//����ﵽ�˷�֧���ޣ��ͽ��з���
	if (ixNode->keynum == order) {
		PF_PageHandle newpageHandle;
		if ((tmp = AllocatePage(indexHandle->fileID, &newpageHandle)) != SUCCESS) {
			return tmp;
		}

		//�����½ڵ㣬�޸Ķ�Ӧ�ĳ�Ա��ֵ
		IX_Node* newixNode = getIXNodefromPH(&newpageHandle);
		newixNode->is_leaf = ixNode->is_leaf;
		newixNode->brother = ixNode->brother;
		ixNode->brother = newpageHandle.pFrame->page.pageNum;
		newixNode->parent = pageHandle.pFrame->page.pageNum;
		newixNode->keys = newpageHandle.pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
		newixNode->rids = (RID*)(newixNode->keys + order * indexinfo.attrLength);

		//��ǰһ�ڵ�ĺ������ת�Ƶ��½ڵ��У�ԭ�пռ����
		memmove(newixNode->keys, ixNode->keys + order / 2 * indexinfo.attrLength, (order + 1) / 2 * indexinfo.attrLength);
		memset(ixNode->keys + order / 2 * indexinfo.attrLength, 0, (order + 1) / 2 * indexinfo.attrLength);
		memmove(newixNode->rids, ixNode->rids + order / 2, (order + 1) / 2 * PTR_SIZE);
		memset(ixNode->rids + order / 2, 0, (order + 1) / 2 * PTR_SIZE);
		newixNode->keynum = (order + 1) / 2;
		ixNode->keynum -= newixNode->keynum;

		//���ݲ���ڵ�λ����ѡ��������½ڵ㻹�Ǿɽڵ���
		if (_index <= order / 2) {
			memmove(ixNode->keys + (_index + 1) * indexinfo.attrLength, ixNode->keys + _index * indexinfo.attrLength, order / 2 * indexinfo.attrLength);
			memmove(ixNode->keys + _index * indexinfo.attrLength, pData, indexinfo.attrLength);
			memmove(ixNode->rids + _index + 1, ixNode->rids + _index, order / 2 * PTR_SIZE);
			memmove(ixNode->rids + _index, rid, PTR_SIZE);
			ixNode->keynum++;
		}
		else {
			_index -= order / 2;
			memmove(newixNode->keys + (_index + 1) * indexinfo.attrLength, newixNode->keys + _index * indexinfo.attrLength, order / 2 * indexinfo.attrLength);
			memmove(newixNode->keys + _index * indexinfo.attrLength, pData, indexinfo.attrLength);
			memmove(newixNode->rids + _index + 1, newixNode->rids + _index, order / 2 * PTR_SIZE);
			memmove(newixNode->rids + _index, rid, PTR_SIZE);
			newixNode->keynum++;
		}

		int pageNum = newpageHandle.pFrame->page.pageNum;
		void* data = newixNode->keys;

		MarkDirty(&newpageHandle);
		UnpinPage(&newpageHandle);

		//�����ǰ���ѵ��Ǹ��ڵ㣬��ô��Ҫ�����µĸ��ڵ�
		if (pageHandle.pFrame->page.pageNum != indexHandle->fileHeader.rootPage) {
			InsertEntryInPage(indexHandle, data, (RID*)&pageNum, ixNode->parent);
		}
		else {
			PF_PageHandle rootpageHandle;
			if ((tmp = AllocatePage(indexHandle->fileID, &rootpageHandle)) != SUCCESS) {
				return tmp;
			}
			IX_Node* rootixNode = getIXNodefromPH(&rootpageHandle);
			rootixNode->keys = rootpageHandle.pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
			rootixNode->rids = (RID*)(rootixNode->keys + order * indexinfo.attrLength);
			rootixNode->brother = 0;
			rootixNode->is_leaf = 0;
			rootixNode->parent = 0;
			indexHandle->fileHeader.rootPage = rootpageHandle.pFrame->page.pageNum;
			ixNode->parent = rootpageHandle.pFrame->page.pageNum;
			newixNode->parent = rootpageHandle.pFrame->page.pageNum;
			memmove(rootixNode->keys, data, indexinfo.attrLength);
			memmove(rootixNode->rids, (RID*)&pageNum, PTR_SIZE);
			rootixNode->keynum++;
		}
	}
	else {
		memmove(ixNode->keys + (_index + 1) * indexinfo.attrLength, ixNode->keys + _index * indexinfo.attrLength, order / 2 * indexinfo.attrLength);
		memmove(ixNode->keys + _index * indexinfo.attrLength, pData, indexinfo.attrLength);
		memmove(ixNode->rids + _index + 1, ixNode->rids + _index, order / 2 * PTR_SIZE);
		memmove(ixNode->rids + _index, rid, PTR_SIZE);
		ixNode->keynum++;
	}

	MarkDirty(&pageHandle);
	UnpinPage(&pageHandle);

	return SUCCESS;
}

