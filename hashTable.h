#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#define TABLE_SIZE 1024

struct KeyValue
{
    long key;
    long value;
    struct KeyValue *next;
};

struct HashTable
{
    struct KeyValue *hashTable[TABLE_SIZE];
};

struct HashTable *createHashTable();
void insertKeyValue(struct HashTable *ht, long key, long value);
long searchKeyValue(struct HashTable *ht, long key);
void printHashTable(struct HashTable *ht);
void freeHashTable(struct HashTable *ht);
int deleteKey(struct HashTable *ht, long key);

#endif // HASH_TABLE_H
