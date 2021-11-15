#include "IX_Manager.h"

//����attryypeָ�������ͱȽ����������ֵ
int CmpValue(AttrType attrType, char* keyval, char* value);

//����B+��������ֵ������value��Ҷ�ӽڵ㣬��������������ҳ���ҳ������ҳ��ź���������
RC FindeIXNode(IX_IndexHandle* indexHandle, char* value, PF_PageHandle* pageHandle, PageNum* pn);

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
	PF_PageHandle* pageHandle = (PF_PageHandle*)malloc(sizeof(PF_PageHandle));
	if ((tmp = AllocatePage(fileID, pageHandle)) != SUCCESS) {
		return tmp;
	}

	IX_FileHeader* fileHeader = (IX_FileHeader*)pageHandle->pFrame->page.pData;
	fileHeader->attrLength = attrLength;
	fileHeader->attrType = attrType;
	fileHeader->keyLength = attrLength + sizeof(RID);
	fileHeader->rootPage = 1;
	fileHeader->first_leaf = 1;
	fileHeader->order = (PF_PAGE_SIZE - sizeof(IX_FileHeader) - sizeof(IX_Node)) / (2 * sizeof(RID) + attrLength);

	IX_Node* IXNode = (IX_Node*)(pageHandle->pFrame->page.pData + sizeof(IX_FileHeader));
	IXNode->is_leaf = 1;
	IXNode->keynum = 0;
	IXNode->parent = 0;
	IXNode->brother = 0;
	IXNode->keys = pageHandle->pFrame->page.pData + sizeof(IX_FileHeader) + sizeof(IX_Node);
	IXNode->rids = (RID*)(IXNode->keys + fileHeader->order * (2 * sizeof(RID) + attrLength));

	//�޸�ҳ������PF�к������д���
	MarkDirty(pageHandle);

	UnpinPage(pageHandle);

	free(pageHandle);

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
	RC tmp;
	AttrType attrType = indexHandle->fileHeader.attrType;
	int attrLength = indexHandle->fileHeader.attrLength;
	int keyLength = indexHandle->fileHeader.keyLength;
	return RC();
}

RC DeleteEntry(IX_IndexHandle* indexHandle, void* pData, const RID* rid)
{
	RC tmp;
	AttrType attrType = indexHandle->fileHeader.attrType;
	int attrLength = indexHandle->fileHeader.attrLength;
	int keyLength = indexHandle->fileHeader.keyLength;
	return RC();
}

RC OpenIndexScan(IX_IndexScan* indexScan, IX_IndexHandle* indexHandle, CompOp compOp, char* value)
{
	if (indexScan->bOpen == true) {
		return IX_SCANOPENNED;
	}

	RC tmp;
	PF_PageHandle pageHandle;
	PageNum pn;
	
	tmp = FindeIXNode(indexHandle, value, &pageHandle, &pn);
	if (tmp != SUCCESS) return tmp;

	IX_Node* ixNode = (IX_Node*)(pageHandle.pFrame->page.pData + sizeof(IX_FileHeader));
	AttrType attrType = indexHandle->fileHeader.attrType;
	int attrLength = indexHandle->fileHeader.attrLength;
	int keyLength = indexHandle->fileHeader.keyLength;
	char* keyval = (char*)malloc(attrLength);

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
			memcpy(keyval, ixNode->keys + ridIx * keyLength, attrLength);
			int result = CmpValue(attrType, keyval, value);
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
				ixNode = (IX_Node*)(pageHandle.pFrame->page.pData + sizeof(IX_FileHeader));
				ridIx = 0;
			}
			memcpy(keyval, ixNode->keys + ridIx * keyLength, attrLength);
			int result = CmpValue(attrType, keyval, value);
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
	IX_Node* ixNode = (IX_Node*)(indexScan->PageHandle.pFrame->page.pData + sizeof(IX_FileHeader));
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
	AttrType attrType = indexScan->pIXIndexHandle->fileHeader.attrType;
	int attrLength = indexScan->pIXIndexHandle->fileHeader.attrLength;
	int keyLength = indexScan->pIXIndexHandle->fileHeader.keyLength;
	char*  keyval = (char*)malloc(attrLength);
	memcpy(keyval, ixNode->keys + indexScan->ridIx * keyLength, attrLength);
	int result = CmpValue(attrType, keyval, indexScan->value);
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


RC FindeIXNode(IX_IndexHandle* indexHandle, char* value, PF_PageHandle* pageHandle, PageNum* pn) {
	//�Ӹ��ڵ㿪ʼ������Ҫ��ֵ
	PageNum _pn = indexHandle->fileHeader.rootPage;

	IX_Node* ixNode;
	AttrType attrType = indexHandle->fileHeader.attrType;
	int attrLength = indexHandle->fileHeader.attrLength;
	int keyLength = indexHandle->fileHeader.keyLength;
	char* keyval = (char*)malloc(attrLength);

	PF_PageHandle _pageHandle;
	RC tmp;
	int _ridIx;
	while(1){
		//ÿ�ζ�ȡҳ��Ϊ_pn��ҳ����д���
		if ((tmp = GetThisPage(indexHandle->fileID, _pn, &_pageHandle)) != SUCCESS) {
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
			memcpy(keyval, ixNode->keys + _ridIx * keyLength, attrLength);
			result = CmpValue(attrType, keyval, value);
			if (result < 0) {
				continue;
			}
			else if (result == 0){
				break;
			}
			else {
				if (_ridIx == 0) {
					return IX_INVALIDKEY;
				}
				_ridIx--;
				break;
			}
		}
		_pn = *(int*)(ixNode->rids + _ridIx);
		UnpinPage(&_pageHandle);
	} 
	*pageHandle = _pageHandle;
	free(keyval);
	return SUCCESS;
}
