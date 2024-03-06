#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <mqueue.h>
#include <string.h>

#include "hashTable.h"

// Preprocessor Constants
#define MQ_NAME_MAXSIZE 32
#define FILE_NAME_MAXSIZE 32

// Global Shared Variables (Threads Only Read)
int DCOUNT;
char *FNAME;
int TCOUNT;
int VSIZE;
char *MQNAME;
int const REQ_LENGTH = 10;
char mqname1[MQ_NAME_MAXSIZE];
char mqname2[MQ_NAME_MAXSIZE];

// Global Shared Variables (Threads Modify)
char *messageBuffer;
int condBuffer;
int active;
int *availableData;
FILE **filePointers;
struct HashTable **hashTables;

// Mutex Variables
pthread_mutex_t mutexBuffer;
pthread_mutex_t *mutexData;

// Conditional Variables
pthread_cond_t condReceive;
pthread_cond_t condProduce;
pthread_cond_t *condData;

void *frontendThreadFunc(void *arg)
{
    // Structs
    struct msgItem
    {
        int clientNo;
        char req[REQ_LENGTH + 1];
        long int key;
        char value[VSIZE];
        int success;
    };

    struct dataItem
    {
        long int key;
        char value[VSIZE];
    };

    // Open & Initialize message queue
    mqd_t messageQueue;
    messageQueue = mq_open(mqname1, O_RDONLY);
    struct mq_attr mqAttributes;
    mq_getattr(messageQueue, &mqAttributes);
    messageBuffer = (char *)malloc(mqAttributes.mq_msgsize);

    while (active)
    {
        // ENTRY SECTION START
        pthread_mutex_lock(&mutexBuffer);
        while (condBuffer != 1)
        {
            pthread_cond_wait(&condReceive, &mutexBuffer);
        }
        // ENTRY SECTION END

        // CRITICAL SECTION START
        mq_receive(messageQueue, messageBuffer, mqAttributes.mq_msgsize, NULL);
        struct msgItem *item = (struct msgItem *)messageBuffer;

        // DUMP Request
        if (strcmp(item->req, "DUMP") == 0)
        {
            // INNER ENTRY SECTION START
            for (int i = 0; i < DCOUNT; i++)
            {
                pthread_mutex_lock(&(mutexData[i]));

                if (availableData[i] != 0)
                {
                    pthread_cond_wait(&(condData[i]), &(mutexData[i]));
                }

                availableData[i] = -1;
                pthread_mutex_unlock(&(mutexData[i]));
            }
            // INNER ENTRY SECTION END

            // INNER CRITICAL SECTION START
            char filename[FILE_NAME_MAXSIZE];
            strcpy(filename, item->value);
            FILE *outputFile = fopen(filename, "w");
            if (outputFile == NULL)
            {
                perror("File could not be opened");
                return 0;
            }

            char storeBuffer[sizeof(struct dataItem)];
            struct dataItem *storePtr = (struct dataItem *)storeBuffer;
            for (int i = 0; i < DCOUNT; i++)
            {
                FILE *fd = filePointers[i];

                struct HashTable *ht = hashTables[i];

                for (int j = 0; j < TABLE_SIZE; j++)
                {
                    struct KeyValue *itemPtr = ht->hashTable[j];

                    while (itemPtr != NULL)
                    {
                        fseek(fd, itemPtr->value, SEEK_SET);
                        fread(storeBuffer, sizeof(struct dataItem), 1, fd);
                        fprintf(outputFile, "%ld %s\n", storePtr->key, storePtr->value);
                        itemPtr = itemPtr->next;
                    }
                }
            }
            fclose(outputFile);

            if (item->key != -1)
            {
                mqd_t messageQueue2;
                messageQueue2 = mq_open(mqname2, O_WRONLY);
                item->success = 1;
                mq_send(messageQueue2, messageBuffer, sizeof(struct msgItem), 0);
                mq_close(messageQueue2);
            }
            // INNER CRITICAL SECTION END

            // INNER EXIT SECTION START
            for (int i = 0; i < DCOUNT; i++)
            {
                pthread_mutex_lock(&(mutexData[i]));
                availableData[i] = 0;
                pthread_cond_signal(&(condData[i]));
                pthread_mutex_unlock(&(mutexData[i]));
            }
            // INNER EXIT SECTION END
            // CRITICAL SECTION END

            // EXIT SECTION START
            pthread_mutex_unlock(&mutexBuffer);
            // EXIT SECTION END

            continue;
        }
        else if (strcmp(item->req, "QUITSERVER") == 0)
        {
            active = 0;
            mqd_t messageQueue2;
            messageQueue2 = mq_open(mqname2, O_WRONLY);
            item->key = 0;
            item->success = 1;
            mq_send(messageQueue2, messageBuffer, sizeof(struct msgItem), 0);
            mq_close(messageQueue2);
        }
        // CRITICAL SECTION END

        // EXIT SECTION START
        condBuffer = 0; // Set buffer available for reading only and not writing
        pthread_cond_signal(&condProduce);
        pthread_mutex_unlock(&mutexBuffer);
        // EXIT SECTION END
    }

    // THREAD EXIT
    pthread_cond_broadcast(&condProduce);
    mq_close(messageQueue);
    pthread_exit(NULL);
}

void *workerThread(void *arg)
{
    // Structs
    struct msgItem
    {
        int clientNo;
        char req[REQ_LENGTH + 1];
        long int key;
        char value[VSIZE];
        int success;
    };

    struct dataItem
    {
        long int key;
        char value[VSIZE];
    };

    // Initialization
    mqd_t messageQueue;
    messageQueue = mq_open(mqname2, O_WRONLY);
    struct mq_attr mqAttributes;
    mq_getattr(messageQueue, &mqAttributes);

    char localMessageBuffer[mqAttributes.mq_msgsize];

    while (1)
    {
        // ENTRY SECTION START
        pthread_mutex_lock(&mutexBuffer);
        while (condBuffer != 0)
        {
            pthread_cond_wait(&condProduce, &mutexBuffer);
        }
        // ENTRY SECTION END

        // CRITICAL SECTION START
        if (!active)
        {
            // EXIT SECTION START
            pthread_mutex_unlock(&mutexBuffer);
            // EXIT SECTION END
            break;
        }
        memcpy(localMessageBuffer, messageBuffer, sizeof(struct msgItem));
        // CRITICAL SECTION END

        // EXIT SECTION START
        condBuffer = 1; // Set buffer available for frontend again
        pthread_cond_signal(&condReceive);
        pthread_mutex_unlock(&mutexBuffer);
        // EXIT SECTION END

        // REMAINDER SECTION START
        struct msgItem *item = (struct msgItem *)localMessageBuffer;
        int dataFile = (item->key % DCOUNT);
        long searchResult;
        int match = 0;
        // REMAINDER SECTION END

        // ENTRY SECTION START
        pthread_mutex_lock(&(mutexData[dataFile]));
        if (strcmp(item->req, "GET") == 0)
        {
            while (availableData[dataFile] < 0)
            {
                pthread_cond_wait(&(condData[dataFile]), &(mutexData[dataFile]));
            }
            availableData[dataFile] += 1; // BEING READ STATE
            pthread_mutex_unlock(&(mutexData[dataFile]));
            // ENTRY SECTION END

            // CRITICAL SECTION START
            searchResult = searchKeyValue(hashTables[dataFile], item->key);

            if (searchResult != -1)
            {
                // Get operations
                FILE *fd = filePointers[dataFile];
                fseek(fd, searchResult, SEEK_SET);
                char storeBuffer[sizeof(struct dataItem)];
                struct dataItem *storePtr = (struct dataItem *)storeBuffer;
                int readResult = fread(storeBuffer, sizeof(struct dataItem), 1, fd);
                strcpy(item->value, storePtr->value);
                item->success = readResult != -1 ? 1 : 0;
                mq_send(messageQueue, localMessageBuffer, sizeof(struct msgItem), 0);
                match = 1;
            }
            // CRITICAL SECTION END

            // EXIT SECTION START
            pthread_mutex_lock(&(mutexData[dataFile]));
            availableData[dataFile] -= 1;
        }
        else if (strcmp(item->req, "PUT") == 0)
        {
            while (availableData[dataFile] != 0)
            {
                pthread_cond_wait(&(condData[dataFile]), &(mutexData[dataFile]));
            }
            availableData[dataFile] = -1; // BEING MODIFIED STATE
            pthread_mutex_unlock(&(mutexData[dataFile]));
            // ENTRY SECTION END

            // CRITICAL SECTION START
            searchResult = searchKeyValue(hashTables[dataFile], item->key);

            // Put operations
            FILE *fd = filePointers[dataFile];

            struct dataItem data;
            data.key = item->key;
            strcpy(data.value, item->value);

            if (searchResult == -1)
            {
                fseek(fd, 0, SEEK_END);
            }
            else
            {
                fseek(fd, searchResult, SEEK_SET);
            }
            long offset = ftell(fd); // Get the current file position
            int writeResult = fwrite(&data, sizeof(struct dataItem), 1, fd);
            item->success = writeResult != -1 ? 1 : 0;
            insertKeyValue(hashTables[dataFile], item->key, offset);
            mq_send(messageQueue, localMessageBuffer, sizeof(struct msgItem), 0);
            match = 1;
            // CRITICAL SECTION END

            // EXIT SECTION START
            pthread_mutex_lock(&(mutexData[dataFile]));
            availableData[dataFile] = 0;
        }
        else if (strcmp(item->req, "DEL") == 0)
        {
            while (availableData[dataFile] != 0)
            {
                pthread_cond_wait(&(condData[dataFile]), &(mutexData[dataFile]));
            }
            availableData[dataFile] = -1; // BEING MODIFIED STATE
            pthread_mutex_unlock(&(mutexData[dataFile]));
            // ENTRY SECTION END

            // CRITICAL SECTION START
            searchResult = searchKeyValue(hashTables[dataFile], item->key);

            // Del operations
            if (searchResult != -1)
            {
                struct dataItem data;
                data.key = 0UL; // Deleted Item
                for (int i = 0; i < VSIZE; i++)
                {
                    data.value[i] = '\0';
                }

                FILE *fd = filePointers[dataFile];
                fseek(fd, searchResult, SEEK_SET);
                int deleteResult = fwrite(&data, sizeof(struct dataItem), 1, fd);
                item->success = deleteResult != -1 ? 1 : 0;

                deleteKey(hashTables[dataFile], item->key);
                mq_send(messageQueue, localMessageBuffer, sizeof(struct msgItem), 0);
                match = 1;
            }
            // CRITICAL SECTION END

            // EXIT SECTION START
            pthread_mutex_lock(&(mutexData[dataFile]));
            availableData[dataFile] = 0;
        }
        pthread_cond_broadcast(&(condData[dataFile]));
        pthread_mutex_unlock(&(mutexData[dataFile]));
        // EXIT SECTION END

        if (!match)
        {
            item->success = 0;
            mq_send(messageQueue, localMessageBuffer, sizeof(struct msgItem), 0);
        }
    }

    // THREAD EXIT
    mq_close(messageQueue);
    pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
    DCOUNT = 5; // (k mod D) + 1 mapping
    FNAME = NULL;
    TCOUNT = 3;
    VSIZE = 32;
    MQNAME = NULL;

    int option;
    while ((option = getopt(argc, argv, "d:f:t:s:m:")) != -1)
    {
        switch (option)
        {
        case 'd':
            DCOUNT = atoi(optarg);
            if (DCOUNT < 1 || DCOUNT > 5)
            {
                fprintf(stderr, "Error: Minimum and maximum values for D are 1 and 5.\n");
                exit(EXIT_FAILURE);
            }
            break;
        case 'f':
            if (optarg == NULL || strcmp(optarg, "") == 0 || optarg[0] == '-')
            {
                fprintf(stderr, "Error: F must not be NULL.\n");
                exit(EXIT_FAILURE);
            }
            FNAME = malloc(strlen(optarg) + 1);
            strcpy(FNAME, optarg);
            break;
        case 't':
            TCOUNT = atoi(optarg);
            if (TCOUNT < 1 || TCOUNT > 5)
            {
                fprintf(stderr, "Error: Minimum and maximum values for T are 1 and 5.\n");
                exit(EXIT_FAILURE);
            }
            break;
        case 's':
            VSIZE = atoi(optarg);
            if (VSIZE % 32 != 0 || VSIZE < 32 || VSIZE > 1024)
            {
                fprintf(stderr, "Error: S should have a value that is multiple of 32, between 32 and 1024.\n");
                exit(EXIT_FAILURE);
            }
            break;
        case 'm':
            if (optarg == NULL || strcmp(optarg, "") == 0 || optarg[0] == '-')
            {
                fprintf(stderr, "Error: M must not be NULL.\n");
                exit(EXIT_FAILURE);
            }
            MQNAME = malloc(strlen(optarg) + 1);
            strcpy(MQNAME, optarg);
            break;
        default:
            fprintf(stderr, "Usage: %s -d dcount -f fname -t tcount -s vsize -m mqname\n", argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    // Variables
    pthread_t frontendThread;
    pthread_t workerThreads[TCOUNT];
    mqd_t messageQueue1, messageQueue2;
    char filename[FILE_NAME_MAXSIZE];

    // Initialization
    sprintf(mqname1, "/%s%d", MQNAME, 1);
    sprintf(mqname2, "/%s%d", MQNAME, 2);
    messageQueue1 = mq_open(mqname1, O_CREAT, 0666, NULL);
    messageQueue2 = mq_open(mqname2, O_CREAT, 0666, NULL);
    if (messageQueue1 == (mqd_t)-1 || messageQueue2 == (mqd_t)-1)
    {
        perror("mq_open");
        printf("Creating mq error, m1 = %d, m2= %d\n", (int)messageQueue1, (int)messageQueue2);
    }
    mq_close(messageQueue1);
    mq_close(messageQueue2);

    active = 1;
    condBuffer = 1;

    pthread_mutex_init(&mutexBuffer, NULL);
    pthread_cond_init(&condReceive, NULL);
    pthread_cond_init(&condProduce, NULL);

    availableData = (int *)malloc(sizeof(int) * DCOUNT);
    mutexData = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t) * DCOUNT);
    condData = (pthread_cond_t *)malloc(sizeof(pthread_cond_t) * DCOUNT);
    hashTables = (struct HashTable **)malloc(sizeof(struct HashTable *) * DCOUNT);
    for (int i = 0; i < DCOUNT; i++)
    {
        pthread_mutex_init(&(mutexData[i]), NULL);
        pthread_cond_init(&(condData[i]), NULL);
        availableData[i] = 0; // All data files available initially
        hashTables[i] = createHashTable();
    }

    int oldFileCount = 0;
    while (1)
    {
        sprintf(filename, "%s%d.bin", FNAME, oldFileCount + 1);

        if (access(filename, F_OK) == 0)
        {
            oldFileCount++;
        }
        else
        {
            break;
        }
    }

    if (oldFileCount != 0)
    {
        printf("WARNING: %d data files found in directory so DCOUNT is set to %d from %d\n", oldFileCount, DCOUNT, oldFileCount);
        fflush(stdout);
        DCOUNT = oldFileCount;
    }

    // Read data store files into hash tables for prior data
    for (int i = 0; i < DCOUNT; i++)
    {
        sprintf(filename, "%s%d.bin", FNAME, i + 1);

        int dataFile = open(filename, O_CREAT | O_RDONLY, 0666);
        if (dataFile == -1)
        {
            perror("File could not be opened");
            return 0;
        }

        struct dataItem
        {
            long int key;
            char value[VSIZE];
        };
        struct dataItem data;
        off_t diff = sizeof(struct dataItem);
        while (read(dataFile, &data, sizeof(struct dataItem)))
        {
            if (data.key != 0L)
            {
                off_t offset = lseek(dataFile, 0, SEEK_CUR);
                insertKeyValue(hashTables[i], data.key, (long)(offset - diff));
            }
        }
        close(dataFile);
    }

    filePointers = malloc(sizeof(FILE *) * DCOUNT);
    for (int i = 0; i < DCOUNT; i++)
    {
        sprintf(filename, "%s%d.bin", FNAME, i + 1);

        filePointers[i] = fopen(filename, "r+");
    }

    // Create threads
    pthread_create(&frontendThread, NULL, frontendThreadFunc, NULL);
    for (long int i = 0; i < TCOUNT; i++)
    {
        pthread_create(&(workerThreads[i]), NULL, workerThread, (void *)i);
    }

    // Join threads
    pthread_join(frontendThread, NULL);
    for (int i = 0; i < TCOUNT; i++)
    {
        pthread_join(workerThreads[i], NULL);
    }

    // Cleaning up
    mq_unlink(mqname1);
    mq_unlink(mqname2);

    free(MQNAME);
    free(FNAME);
    free(messageBuffer);
    free(availableData);

    pthread_mutex_destroy(&mutexBuffer);
    pthread_cond_destroy(&condReceive);
    pthread_cond_destroy(&condProduce);

    for (int i = 0; i < DCOUNT; ++i)
    {
        pthread_mutex_destroy(&mutexData[i]);
        pthread_cond_destroy(&condData[i]);
    }

    free(mutexData);
    free(condData);

    for (int i = 0; i < DCOUNT; i++)
    {
        freeHashTable(hashTables[i]);
        fclose(filePointers[i]);
    }
    free(hashTables);
    free(filePointers);
}