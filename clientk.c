#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <mqueue.h>
#include <string.h>

// Preprocessor Constants
#define MQ_NAME_MAXSIZE 32
#define FILE_NAME_MAXSIZE 32
#define CHAR_SIZE_OF_LONG_INT 20

// Global Shared Variables (Threads Only Read)
int CLICOUNT;
char *FNAME;
int DLEVEL;
int VSIZE;
char *MQNAME;
int const REQ_LENGTH = 10;
char mqname1[MQ_NAME_MAXSIZE];
char mqname2[MQ_NAME_MAXSIZE];

// Global Shared Variables (Threads Modify)
char *messageBuffer;
int condBuffer;
int clientCount;
int active;

// Mutex Variables
pthread_mutex_t mutexBuffer;
pthread_mutex_t mutexCount;

// Conditional Variables
pthread_cond_t condReceive;
pthread_cond_t *condProduceClient;

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

    // Open & Initialize message queue
    mqd_t messageQueue;
    messageQueue = mq_open(mqname2, O_RDONLY);
    struct mq_attr mqAttributes;
    mq_getattr(messageQueue, &mqAttributes);
    messageBuffer = (char *)malloc(mqAttributes.mq_msgsize);

    while (active)
    {
        // ENTRY SECTION START
        pthread_mutex_lock(&mutexBuffer);
        while (condBuffer != -1)
        {
            pthread_cond_wait(&condReceive, &mutexBuffer);
        }
        // ENTRY SECTION END

        // CRITICAL SECTION START
        mq_receive(messageQueue, messageBuffer, mqAttributes.mq_msgsize, NULL);
        struct msgItem *item = (struct msgItem *)messageBuffer;

        if (strcmp(item->req, "QUIT") == 0 || strcmp(item->req, "QUITSERVER") == 0)
        {
            active = 0;
        }
        // CRITICAL SECTION END

        // EXIT SECTION START
        condBuffer = item->clientNo;
        pthread_cond_signal(&(condProduceClient[condBuffer]));
        pthread_mutex_unlock(&mutexBuffer);
        // EXIT SECTION END
    }

    // THREAD EXIT
    mq_close(messageQueue);
    pthread_exit(NULL);
}

void *clientThreadFunc(void *arg)
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

    // Initialization
    long int tmpId = (long int)arg;
    int threadId = (int)tmpId;
    int readBufferSize = (REQ_LENGTH + 1) + (CHAR_SIZE_OF_LONG_INT + 1) + (VSIZE + 1) + 1;
    char readBuffer[readBufferSize];

    mqd_t messageQueue;
    messageQueue = mq_open(mqname1, O_WRONLY);
    struct mq_attr mqAttributes;
    mq_getattr(messageQueue, &mqAttributes);

    char localMessageBuffer[mqAttributes.mq_msgsize];

    char filename[FILE_NAME_MAXSIZE];
    sprintf(filename, "%s%d.txt", FNAME, threadId + 1);
    FILE *inputFile = fopen(filename, "r");

    while (fgets(readBuffer, readBufferSize, inputFile))
    {
        struct msgItem item;
        item.clientNo = threadId;
        item.key = 0;

        for (int i = 0; i < VSIZE; i++)
        {
            item.value[i] = '\0';
        }

        sscanf(readBuffer, "%s %ld %[a-zA-Z0-9]", item.req, &item.key, item.value);
        memcpy(localMessageBuffer, &item, sizeof(struct msgItem));

        mq_send(messageQueue, localMessageBuffer, sizeof(struct msgItem), 0);

        // ENTRY SECTION START
        pthread_mutex_lock(&mutexBuffer);
        while (condBuffer != threadId)
        {
            pthread_cond_wait(&(condProduceClient[threadId]), &mutexBuffer);
        }
        // ENTRY SECTION START

        // CRITICAL SECTION START
        struct msgItem *reply = (struct msgItem *)messageBuffer;

        if (DLEVEL)
        {
            if (strcmp(reply->req, "GET") == 0)
            {
                printf("Reply --> %s Request (Key: %ld) is %s %s\n", reply->req, reply->key, reply->success ? "SUCCESSFUL Extracted Value:" : "NOT SUCCESSFUL", reply->value);
            }
            else if (strcmp(reply->req, "PUT") == 0)
            {
                printf("Reply --> %s Request (Key: %ld Value: %s) is %s\n", reply->req, reply->key, reply->value, reply->success ? "SUCCESSFUL" : "NOT SUCCESSFUL");
            }
            else if (strcmp(reply->req, "DEL") == 0)
            {
                printf("Reply --> %s Request (Key: %ld) is %s\n", reply->req, reply->key, reply->success ? "SUCCESSFUL" : "NOT SUCCESSFUL");
            }
            else
            {
                printf("Reply --> UNKNOWN REQUEST: %s\n", reply->req);
            }
        }
        // CRITICAL SECTION END

        // EXIT SECTION START
        condBuffer = -1; // Set buffer to available for frontend again
        pthread_cond_signal(&condReceive);
        pthread_mutex_unlock(&mutexBuffer);
        // EXIT SECTION END
    }

    // ENTRY SECTION START
    pthread_mutex_lock(&mutexCount);
    // ENTRY SECTION END

    // CRITICAL SECTION START
    clientCount--;
    if (clientCount == 0)
    {
        mqd_t messageQueue2;
        messageQueue2 = mq_open(mqname2, O_WRONLY);

        struct msgItem quitMsg;
        quitMsg.clientNo = 0;
        strcpy(quitMsg.req, "DUMP");
        quitMsg.key = -1; // For no reply
        strcpy(quitMsg.value, "datastoredump.txt");
        mq_send(messageQueue, (char *)&quitMsg, sizeof(struct msgItem), 0);

        strcpy(quitMsg.req, "QUIT");
        mq_send(messageQueue2, (char *)&quitMsg, sizeof(struct msgItem), 0);
        mq_close(messageQueue2);
    }
    // CRITICAL SECTION END

    // EXIT SECTION START
    pthread_mutex_unlock(&mutexCount);
    // EXIT SECTION END

    fclose(inputFile);
    mq_close(messageQueue);
    pthread_exit(NULL);
}

void *interactiveThreadFunc(void *arg)
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

    // Initialization
    int threadId = 0;
    int readBufferSize = (REQ_LENGTH + 1) + (CHAR_SIZE_OF_LONG_INT + 1) + (VSIZE + 1) + 1;
    char readBuffer[readBufferSize];

    mqd_t messageQueue;
    messageQueue = mq_open(mqname1, O_WRONLY);
    struct mq_attr mqAttributes;
    mq_getattr(messageQueue, &mqAttributes);

    char localMessageBuffer[mqAttributes.mq_msgsize];

    while (1)
    {
        printf("Request: ");
        fgets(readBuffer, readBufferSize, stdin);

        // End command
        if (strcmp(readBuffer, "QUIT\n") == 0)
        {
            pthread_mutex_lock(&mutexCount); // ENTRY SECTION

            // CRITICAL SECTION START
            clientCount--;
            if (clientCount == 0)
            {
                mqd_t messageQueue2;
                messageQueue2 = mq_open(mqname2, O_WRONLY);

                struct msgItem quitMsg;
                quitMsg.clientNo = 0;
                strcpy(quitMsg.req, "QUIT");

                mq_send(messageQueue2, (char *)&quitMsg, sizeof(struct msgItem), 0);
                mq_close(messageQueue2);
            }
            // CRITICAL SECTION END

            pthread_mutex_unlock(&mutexCount); // EXIT SECTION
            break;
        }

        // Create item
        struct msgItem item;
        item.clientNo = threadId;
        for (int i = 0; i < VSIZE; i++)
        {
            item.value[i] = '\0';
        }

        sscanf(readBuffer, "%s %ld %[a-zA-Z0-9]", item.req, &item.key, item.value);

        // Dump Request Case
        if (strcmp(item.req, "DUMP") == 0)
        {
            sscanf(readBuffer, "%s %[^ \n]", item.req, item.value);
            item.key = 0;

            if (strcmp(item.value, "") == 0)
            {
                strcpy(item.value, "output.txt");
            }
        }

        memcpy(localMessageBuffer, &item, sizeof(struct msgItem));
        mq_send(messageQueue, localMessageBuffer, sizeof(struct msgItem), 0);

        pthread_mutex_lock(&mutexBuffer); // ENTRY SECTION

        // CRITICAL SECTION START
        while (condBuffer != threadId)
        {
            pthread_cond_wait(&(condProduceClient[threadId]), &mutexBuffer);
        }

        struct msgItem *reply = (struct msgItem *)messageBuffer;

        if (strcmp(reply->req, "QUITSERVER") == 0)
        {
            break;
        }

        if (DLEVEL)
        {
            if (strcmp(reply->req, "GET") == 0)
            {
                printf("Reply --> %s Request (Key: %ld) is %s %s\n", reply->req, reply->key, reply->success ? "SUCCESSFUL Extracted Value:" : "NOT SUCCESSFUL", reply->value);
            }
            else if (strcmp(reply->req, "PUT") == 0)
            {
                printf("Reply --> %s Request (Key: %ld Value: %s) is %s\n", reply->req, reply->key, reply->value, reply->success ? "SUCCESSFUL" : "NOT SUCCESSFUL");
            }
            else if (strcmp(reply->req, "DEL") == 0)
            {
                printf("Reply --> %s Request (Key: %ld) is %s\n", reply->req, reply->key, reply->success ? "SUCCESSFUL" : "NOT SUCCESSFUL");
            }
            else if (!(strcmp(reply->req, "DUMP") == 0 || strcmp(reply->req, "QUIT") == 0 || strcmp(reply->req, "QUITSERVER") == 0))
            {
                printf("Reply --> UNKNOWN REQUEST: %s\n", reply->req);
            }
        }

        condBuffer = -1; // Set buffer to available for frontend again
        pthread_cond_signal(&condReceive);
        // CRITICAL SECTION END

        pthread_mutex_unlock(&mutexBuffer); // EXIT SECTION
    }

    // Close message queue
    mq_close(messageQueue);
    pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
    CLICOUNT = 0;
    FNAME = "filename";
    DLEVEL = 1;
    VSIZE = 32;
    MQNAME = "/mq";

    int option;
    while ((option = getopt(argc, argv, "n:f:s:m:d:")) != -1)
    {
        switch (option)
        {
        case 'n':
            CLICOUNT = atoi(optarg);
            if (CLICOUNT < 0 || CLICOUNT > 10)
            {
                fprintf(stderr, "Error: Minimum and maximum values for N are 0 and 10.\n");
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
        case 'd':
            DLEVEL = atoi(optarg);
            if (DLEVEL < 0 || DLEVEL > 1)
            {
                fprintf(stderr, "Error: Minimum and maximum values for D are 0 and 1.\n");
                exit(EXIT_FAILURE);
            }
            break;
        default:
            fprintf(stderr, "Usage: %s -n clicount -f fname -s vsize -m mqname -d dlevel\n", argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    // Variables
    pthread_t frontendThread;
    pthread_t clientThread[CLICOUNT];
    pthread_t interactiveThread;

    // Initialization
    sprintf(mqname1, "/%s%d", MQNAME, 1);
    sprintf(mqname2, "/%s%d", MQNAME, 2);

    condBuffer = -1;
    active = 1;
    clientCount = CLICOUNT == 0 ? 1 : CLICOUNT;

    pthread_mutex_init(&mutexBuffer, NULL);
    pthread_cond_init(&condReceive, NULL);

    if (CLICOUNT > 0)
    {
        condProduceClient = (pthread_cond_t *)malloc(sizeof(pthread_cond_t) * CLICOUNT);
        for (int i = 0; i < CLICOUNT; i++)
        {
            pthread_cond_init(&(condProduceClient[i]), NULL);
        }

        pthread_create(&frontendThread, NULL, frontendThreadFunc, NULL);
        for (long int i = 0; i < CLICOUNT; i++)
        {
            pthread_create(&(clientThread[i]), NULL, clientThreadFunc, (void *)i);
        }

        pthread_join(frontendThread, NULL);
        for (int i = 0; i < CLICOUNT; i++)
        {
            pthread_join(clientThread[i], NULL);
        }
    }
    else
    {
        condProduceClient = (pthread_cond_t *)malloc(sizeof(pthread_cond_t) * 1);
        pthread_cond_init(&(condProduceClient[0]), NULL);
        pthread_create(&frontendThread, NULL, frontendThreadFunc, NULL);
        pthread_create(&interactiveThread, NULL, interactiveThreadFunc, NULL);

        pthread_join(frontendThread, NULL);
        pthread_join(interactiveThread, NULL);
    }

    // Clean Up
    free(MQNAME);
    free(FNAME);
    free(messageBuffer);

    pthread_mutex_destroy(&mutexBuffer);
    pthread_mutex_destroy(&mutexCount);
    pthread_cond_destroy(&condReceive);

    for (int i = 0; i < CLICOUNT; ++i)
    {
        pthread_cond_destroy(&condProduceClient[i]);
    }
    
    free(condProduceClient);
}