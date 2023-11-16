/**
 * @file secondary_server.c
 * @author kumarasamy chelliah and anagha g
 * @brief
 * @version 0.1
 * @date 2023-11-10
 *
 * @copyright Copyright (c) 2023
 *
 */

/*
To do:
1. Add pthread_barrier shit
2. array size
3. Check queue
*/
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <semaphore.h>

#define MESSAGE_LENGTH 100
#define LOAD_BALANCER_CHANNEL 4000
#define PRIMARY_SERVER_CHANNEL 4001
#define SECONDARY_SERVER_CHANNEL_1 4002
#define SECONDARY_SERVER_CHANNEL_2 4003
#define MAX_THREADS 200
#define MAX_VERTICES 100
#define MAX_QUEUE_SIZE 100

/**
 * This structure, struct data, is used to store message data. It includes sequence numbers, operation codes, a graph name, and arrays for storing BFS sequence and its length.
 */
struct data
{
    long seq_num;
    long operation;
    char graph_name[MESSAGE_LENGTH];
};

/**
 * The struct msg_buffer is used for message passing through message queues. It contains a message type and the struct data structure to carry the message data.
 */
struct msg_buffer
{
    long msg_type;
    struct data data;
};

/**
 * Used to pass data to threads for BFS and dfs processing.
 * It includes a message queue ID and a message buffer.
 * Starting vertex is the vertex from which the BFS or DFS starts.
 * Storage pointer is used to store the SHM pointer address.
 * Number of nodes is the number of nodes in the graph.
 * Adjacency matrix is the adjacency matrix of the graph
 * Visited is an array to keep track of visited nodes.
 */
 
struct current_vertex_data
{
	int current_vertex;
};

// Create and Initialize queue
struct Queue *createQueue()
{
    struct Queue *queue = (struct Queue *)malloc(sizeof(struct Queue));
    queue->front = -1;
    queue->rear = -1;
    return queue;
}

int isEmpty(struct Queue *queue)
{
    return (queue->front == -1);
}

void enqueue(struct Queue *queue, int value)
{
    if (queue->rear == MAX_VERTICES - 1)
    {
        printf("Queue is full\n");
        return;
    }
    if (queue->front == -1)
    {
        queue->front = 0;
    }
    queue->node_size[queue->rear++] = value;
}

int dequeue(struct Queue *queue)
{
    if (isEmpty(queue))
    {
        printf("Queue is empty\n");
        exit(EXIT_FAILURE);
    }
    int value = queue->node_size[queue->front];
    if (queue->front == queue->rear)
    {
        queue->front = -1;
        queue->rear = -1;
    }
    else
    {
        queue->front++;
    }
    return value;
}

struct data_to_thread
{
    int msg_queue_id;
    struct msg_buffer msg;
    struct Queue queue;
    int index;
    int number_of_nodes;
    int **adjacency_matrix;
    int *visited;
    //int *arrayQueue;
    pthread_mutex_t mutexLock; 
    pthread_barrier_t bfs_barrier;
    //note the change
    struct current_vertex_data currentVertexData;
};

void *bfs_subthread(void *arg)
{
	struct data_to_thread *dtt = (struct data_to_thread *)arg;
	//Lock
	pthread_mutex_lock(&dtt->mutexLock);
	dtt->msg.data.graph_name[dtt->index] = dtt->currentVertexData.current_vertex;
	dtt->index++;
	dtt->msg.data.graph_name[dtt->index]= '*';
    //Create Queue
    dtt->queue.bfs_queue= *createQueue();

	//Unlock
	pthread_mutex_unlock(&dtt->mutexLock);
	dtt->visited[dtt->currentVertexData.current_vertex]=1;
	//Loop
	for(int i=0; i<dtt->number_of_nodes; i++)
	{
		if((dtt->adjacency_matrix[dtt->currentVertexData.current_vertex][i]==1) or (dtt->visited[i]==0)
        {
            enqueue(bfs_queue, i);
        }
    }

    //Exit
    printf("Secondary Server: Exiting the BFS Thread");
    pthread_exit(NULL);
}

void *bfs_mainthread(void *arg)
{
    struct data_to_thread *dtt = (struct data_to_thread *)arg;
    dtt->queue.bfs_queue= *createQueue();
    //Connect to shared memory
    key_t shm_key;
    int shm_id;
    int *shmptr;
    // Generate key for the shared memory
    // Here, we are using the seq_num as the key because
    // we want to ensure that each request has a unique shared memory
    while ((shm_key = ftok(".", dtt->msg.data.seq_num)) == -1)
    {
        perror("[Secondary Server] Error while generating key for shared memory");
        exit(EXIT_FAILURE);
    }
    printf("[Secondary Server] Generated shared memory key %d\n", shm_key);

    // Connect to the shared memory using the key
    if ((shm_id = shmget(shm_key, sizeof(dtt->number_of_nodes), 0666)) < 0)
    {
        perror("[Secondary Server] Error occurred while connecting to shm\n");
        exit(EXIT_FAILURE);
    }
    // Attach to the shared memory
    if ((shmptr = (int *)shmat(shm_id, NULL, 0)) == (void *)-1)
    {
        perror("[Secondary Server] Error in shmat \n");
        exit(EXIT_FAILURE);
    }
    dtt->currentVertexData.current_vertex= *shmptr;
    //Opening, reading
    FILE *fptr = fopen(dtt->msg.data.graph_name, "r");
    if (fptr == NULL)
    {
        printf("[Seconday Server] Error opening file");
        exit(EXIT_FAILURE);
    }
    fscanf(fptr, "%d", &(dtt->number_of_nodes));

    dtt->adjacency_matrix = (int **)malloc(dtt->number_of_nodes * sizeof(int *));
    for (int i = 0; i < dtt->number_of_nodes; i++)
    {
        dtt->adjacency_matrix[i] = (int *)malloc(dtt->number_of_nodes * sizeof(int));
    }

    for (int i = 0; i < dtt->number_of_nodes; i++)
    {
        for (int j = 0; j < dtt->number_of_nodes; j++)
        {
            fscanf(fptr, "%d", &(dtt->adjacency_matrix[i][j]));
        }
    }
    fclose(fptr);

    int starting_vertex= dtt->currentVertexData.current_vertex;
    dtt->visited[starting_vertex]=1;
    //push starting vertex into queue
    enqueue(bfs_queue, starting_vertex);

    while(!isEmpty(bfs_queue))
    {
        //fix this
        int array[queue.size];
        //Copy entries into array, empty queue
        int entry=0;
        array[entry++]= dequeue(bfs_queue);
        pthread_t thread_ids[array.size];

        for(int i=0; i<array.size; i++)
        {
            //create new struct vertex
            pthread_create(&thread_ids[i], NULL< bfs_subthread, (void *) dtt)
        }

        for (int i = 0; i < dtt->number_of_nodes; i++)
        {
            pthread_join(subthread_id[i], NULL);
        }

    }

    //Sending shit to client
    dtt->msg.msg_type = dtt->msg.data.seq_num;
    dtt->msg.data.operation = 0;

    printf("[Secondary Server] Sending reply to the client %ld @ %d\n", dtt->msg.msg_type, dtt->msg_queue_id);

    if (msgsnd(dtt->msg_queue_id, &(dtt->msg), sizeof(dtt->msg.data), 0) == -1)
    {
        perror("[Secondary Server] Message could not be sent, please try again");
        exit(EXIT_FAILURE);
    }

    // Detach from the shared memory
    if (shmdt(shmptr) == -1)
    {
        perror("[Secondary Server] Could not detach from shared memory\n");
        exit(EXIT_FAILURE);
    }

    // Exit the DFS thread
    printf("[Secondary Server] DFS Request: Exiting DFS Request\n");
    printf("[Secondary Server] Successfully Completed Operation 3\n");
    pthread_exit(NULL);
}

	

