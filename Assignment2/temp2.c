/**
 * Standard queue DS
 */
struct Queue
{
    int node_size[MAX_VERTICES];
    int front, rear;
};

/**
 * We use this structure to pass data to individual threads. It contains a struct BFSNode for the node being processed, a struct BFSResult to store results, and a struct data_to_thread to pass message data.
 */

struct thread_data
{
    struct BFSNode node;
    struct BFSResult result;
    struct data_to_thread dtt;
};

// Array of thread ids for BFS
pthread_t thread_ids[MAX_THREADS];

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
