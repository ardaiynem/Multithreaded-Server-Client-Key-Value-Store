#include "hashTable.h"
#include <stdlib.h>
#include <stdio.h>

struct HashTable *createHashTable()
{
    struct HashTable *ht = (struct HashTable *)malloc(sizeof(struct HashTable));
    if (ht != NULL)
    {
        for (int i = 0; i < TABLE_SIZE; ++i)
        {
            ht->hashTable[i] = NULL;
        }
    }
    return ht;
}

void insertKeyValue(struct HashTable *ht, long key, long value)
{
    unsigned int index = (unsigned int)(key % TABLE_SIZE);
    struct KeyValue *current = ht->hashTable[index];
    while (current != NULL)
    {
        if (current->key == key)
        {
            current->value = value;
            return;
        }
        current = current->next;
    }

    struct KeyValue *newItem = (struct KeyValue *)malloc(sizeof(struct KeyValue));
    if (newItem != NULL)
    {
        newItem->key = key;
        newItem->value = value;
        newItem->next = ht->hashTable[index];
        ht->hashTable[index] = newItem;
    }
}

int deleteKey(struct HashTable *ht, long key)
{
    unsigned int index = (unsigned int)(key % TABLE_SIZE);
    struct KeyValue *current = ht->hashTable[index];
    struct KeyValue *prev = NULL;

    while (current != NULL)
    {
        if (current->key == key)
        {
            if (prev == NULL)
            {
                ht->hashTable[index] = current->next;
            }
            else
            {
                prev->next = current->next;
            }
            free(current);
            return 1; // Key deleted successfully
        }
        prev = current;
        current = current->next;
    }
    return 0; // Key not found
}

long searchKeyValue(struct HashTable *ht, long key)
{
    unsigned int index = (unsigned int)(key % TABLE_SIZE);
    struct KeyValue *current = ht->hashTable[index];
    while (current != NULL)
    {
        if (current->key == key)
        {
            return current->value;
        }
        current = current->next;
    }
    return -1; // Key not found
}

void printHashTable(struct HashTable *ht)
{
    for (int i = 0; i < TABLE_SIZE; i++)
    {
        struct KeyValue *itemPtr = ht->hashTable[i];

        while (itemPtr != NULL)
        {
            printf("TABLE Index: %d Key: %ld Val: %ld\n", i, itemPtr->key, itemPtr->value);
            itemPtr = itemPtr->next;
        }
    }
}

void freeHashTable(struct HashTable *ht)
{
    for (int i = 0; i < TABLE_SIZE; ++i)
    {
        struct KeyValue *current = ht->hashTable[i];
        while (current != NULL)
        {
            struct KeyValue *next = current->next;
            free(current);
            current = next;
        }
    }
    free(ht);
}
