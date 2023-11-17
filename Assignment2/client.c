/**
 * @file client.c
 * @author Divyateja Pasupuleti
 * @brief
 * @version 0.1
 * @date 2023-11-04
 *
 * @copyright Copyright (c) 2023
 * POSIX-compliant C program client.c
 *
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

struct data
{
    long seq_num;
    long operation;
    char graph_name[MESSAGE_LENGTH];
};

struct msg_buffer
{
    long msg_type;
    struct data data;
};

/**
 * @brief
 *
 * @param msg_queue_id
 * @param seq_num
 * @param message
 */
void operation_one(int msg_queue_id, int seq_num, struct msg_buffer message)
{
    // Input number of nodes
    int number_of_nodes;
    printf("Enter Number of Nodes: ");
    scanf("%d", &number_of_nodes);

    // Input adjacency matrix
    int adjacency_matrix[number_of_nodes][number_of_nodes];
    printf("Enter adjacency matrix, each row on a separate line and elements of a single row separated by whitespace characters: \n");
    for (int i = 0; i < number_of_nodes; i++)
    {
        for (int j = 0; j < number_of_nodes; j++)
        {
            scanf("%d", &adjacency_matrix[i][j]);
        }
    }

    // Connect to shared memory
    key_t shm_key;
    int shm_id;
    // Generate key for the shared memory
    // Here, we are using the client_id as the key because
    // we want to ensure that each client has a unique shared memory
    while ((shm_key = ftok(".", seq_num)) == -1)
    {
        perror("[Client] Error while generating key for shared memory");
        exit(EXIT_FAILURE);
    }
    printf("[Client] Generated shared memory key %d\n", shm_key);
    // Connect to the shared memory using the key
    if ((shm_id = shmget(shm_key, sizeof(adjacency_matrix) + sizeof(number_of_nodes), 0666 | IPC_CREAT)) == -1)
    {
        perror("[Client] Error occurred while connecting to shm\n");
        exit(EXIT_FAILURE);
    }
    // Attach to the shared memory
    int *shmptr = (int *)shmat(shm_id, NULL, 0);
    if (shmptr == (void *)-1)
    {
        perror("[Client] Error while attaching to shared memory\n");
        exit(EXIT_FAILURE);
    }

    int shmptr_index = 0;
    // Store data in shared memory using array traversals
    shmptr[shmptr_index++] = number_of_nodes;
    for (int i = 0; i < number_of_nodes; i++)
    {
        for (int j = 0; j < number_of_nodes; j++)
        {
            shmptr[shmptr_index++] = adjacency_matrix[i][j];
        }
    }

    // Change message channel to load balancer and send it to load balancer
    message.msg_type = LOAD_BALANCER_CHANNEL;
    message.data.operation = 1;
    message.data.seq_num = seq_num;

    // Send the message to the load balancer
    if (msgsnd(msg_queue_id, &message, sizeof(message.data), 0) == -1)
    {
        perror("[Client] Message could not be sent, please try again");
        exit(EXIT_FAILURE);
    }
    else
    {
        while (msgrcv(msg_queue_id, &message, sizeof(message.data), seq_num, 0) == -1)
        {
            perror("[Client] Error while receiving message from Primary server");
        }
        printf("[Client] Message received from the Primary Server: %ld -> %s using %ld\n", message.msg_type, message.data.graph_name, message.data.operation);
        printf("[Client] File written successfully");
    }

    // Detach shared memory and delete it
    if (shmdt(shmptr) == -1)
    {
        perror("[Client] Could not detach from shared memory\n");
        exit(EXIT_FAILURE);
    }
    if (shmctl(shm_id, IPC_RMID, 0) == -1)
    {
        perror("[Client] Error while deleting the shared memory\n");
        exit(EXIT_FAILURE);
    }
}

/**
 * @brief
 *
 * @param msg_queue_id
 * @param seq_num
 * @param message
 */
void operation_three(int msg_queue_id, int seq_num, struct msg_buffer message)
{
    // Input starting vertex
    int starting_vertex;
    printf("Enter Starting Vertex: \n");
    scanf("%d", &starting_vertex);

    // Connect to shared memory
    key_t shm_key;
    int shm_id;
    // Generate key for the shared memory
    // Here, we are using the client_id as the key because
    // we want to ensure that each client has a unique shared memory
    while ((shm_key = ftok(".", seq_num)) == -1)
    {
        perror("[Client] Error while generating key for shared memory");
        exit(EXIT_FAILURE);
    }
    printf("[Client] Generated shared memory key %d\n", shm_key);
    // Connect to the shared memory using the key
    if ((shm_id = shmget(shm_key, sizeof(starting_vertex), 0666 | IPC_CREAT)) == -1)
    {
        perror("[Client] Error occurred while connecting to shm\n");
        exit(EXIT_FAILURE);
    }
    // Attach to the shared memory
    int *shmptr = (int *)shmat(shm_id, NULL, 0);
    if (shmptr == (void *)-1)
    {
        perror("[Client] Error while attaching to shared memory\n");
        exit(EXIT_FAILURE);
    }

    int shmptr_index = 0;
    // Store data in shared memory using array traversals
    shmptr[shmptr_index++] = (starting_vertex - 1);

    // Change message channel to load balancer and sendoperation_three it to load balancer
    message.msg_type = LOAD_BALANCER_CHANNEL;
    message.data.operation = 3;
    message.data.seq_num = seq_num;

    // Send the message to the load balancer
    if (msgsnd(msg_queue_id, &message, sizeof(message.data), 0) == -1)
    {
        perror("[Client] Message could not be sent, please try again");
        exit(EXIT_FAILURE);
    }
    else
    {
        while (msgrcv(msg_queue_id, &message, sizeof(message.data), seq_num, 0) == -1)
        {
            perror("[Client] Error while receiving message from secondary server");
        }
        printf("[Client] Message received from the secondary Server: %ld\nThe list of Leaf Nodes while travelling from %d is: \n", message.msg_type, starting_vertex);
        int i = 0;

        while (message.data.graph_name[i] != '*')
        {
            printf("%d ", message.data.graph_name[i]);
            i++;
        }
        printf("\n[Client] Operation done successfully\n");
    }

    // Detach shared memory and delete it
    if (shmdt(shmptr) == -1)
    {
        perror("[Client] Could not detach from shared memory\n");
        exit(EXIT_FAILURE);
    }
    if (shmctl(shm_id, IPC_RMID, 0) == -1)
    {
        perror("[Client] Error while deleting the shared memory\n");
        exit(EXIT_FAILURE);
    }
}

/**
 * @brief
 *
 * @param msg_queue_id
 * @param seq_num
 * @param message
 */
void operation_four(int msg_queue_id, int seq_num, struct msg_buffer message)
{
    // Input starting vertex
    int starting_vertex;
    printf("Enter Starting Vertex: \n");
    scanf("%d", &starting_vertex);

    // Connect to shared memory
    key_t shm_key;
    int shm_id;
    // Generate key for the shared memory
    // Here, we are using the client_id as the key because
    // we want to ensure that each client has a unique shared memory
    while ((shm_key = ftok(".", seq_num)) == -1)
    {
        perror("[Client] Error while generating key for shared memory");
        exit(EXIT_FAILURE);
    }
    printf("[Client] Generated shared memory key %d\n", shm_key);
    // Connect to the shared memory using the key
    if ((shm_id = shmget(shm_key, sizeof(starting_vertex), 0666 | IPC_CREAT)) == -1)
    {
        perror("[Client] Error occurred while connecting to shm\n");
        exit(EXIT_FAILURE);
    }
    // Attach to the shared memory
    int *shmptr = (int *)shmat(shm_id, NULL, 0);
    if (shmptr == (void *)-1)
    {
        perror("[Client] Error while attaching to shared memory\n");
        exit(EXIT_FAILURE);
    }

    int shmptr_index = 0;
    // Store data in shared memory using array traversals
    shmptr[shmptr_index++] = (starting_vertex - 1);

    // Change message channel to load balancer and sendoperation_three it to load balancer
    message.msg_type = LOAD_BALANCER_CHANNEL;
    message.data.operation = 4;
    message.data.seq_num = seq_num;

    // Send the message to the load balancer
    if (msgsnd(msg_queue_id, &message, sizeof(message.data), 0) == -1)
    {
        perror("[Client] Message could not be sent, please try again");
        exit(EXIT_FAILURE);
    }
    else
    {
        while (msgrcv(msg_queue_id, &message, sizeof(message.data), seq_num, 0) == -1)
        {
            perror("[Client] Error while receiving message from secondary server");
        }
        printf("[Client] Message received from the secondary Server: %ld\nRequired BFS traversal while starting from  %d is: \n", message.msg_type, starting_vertex);
        int i = 0;

        while (message.data.graph_name[i] != '*')
        {
            printf("%d ", message.data.graph_name[i]);
            i++;
        }
        printf("\n[Client] Operation done successfully\n");
    }

    // Detach shared memory and delete it
    if (shmdt(shmptr) == -1)
    {
        perror("[Client] Could not detach from shared memory\n");
        exit(EXIT_FAILURE);
    }
    if (shmctl(shm_id, IPC_RMID, 0) == -1)
    {
        perror("[Client] Error while deleting the shared memory\n");
        exit(EXIT_FAILURE);
    }
}


/**
 * @brief On execution, each instance of this program creates a separate client process,
 * i.e., if the executable file corresponding to client.c is client.out, then each time
 * client.out is executed on a separate terminal, a separate client process is
 * created.
 *
 * @return int 0
 */
int main()
{
    // Initialize the client
    printf("[Client] Initializing Client...\n");

    key_t key;
    int msg_queue_id;
    struct msg_buffer message;

    // Generate key for the message queue
    while ((key = ftok(".", 'B')) == -1)
    {
        perror("[Client] Error while generating key of the file");
        exit(EXIT_FAILURE);
    }

    // Connect to the messsage queue
    while ((msg_queue_id = msgget(key, 0644)) == -1)
    {
        perror("[Client] Error while connecting with Message Queue");
        exit(EXIT_FAILURE);
    }

    printf("[Client] Successfully connected to the Message Queue %d %d\n", key, msg_queue_id);

    // Display the menu
    while (1)
    {
        printf("\n");
        printf("Choose from one of the options below: \n");
        printf("1. Add a new graph to the database\n");
        printf("2. Modify an existing graph of the database\n");
        printf("3. Perform DFS on an existing graph of the database\n");
        printf("4. Perform BFS on an existing graph of the database\n");
        printf("5. Exit\n");

        int operation;
        printf("Enter Operation Number: ");
        scanf("%d", &operation);

        if (operation == 5)
        {
            exit(EXIT_SUCCESS);
        }

        int seq_num;
        printf("Enter Sequence Number: ");
        scanf("%d", &seq_num);

        printf("Enter Graph Name: ");
        scanf("%s", message.data.graph_name);

        printf("\nInput given: Seq: %d Op: %d Name: %s\n", seq_num, operation, message.data.graph_name);

        if (operation == 1 || operation == 2)
        {
            operation_one(msg_queue_id, seq_num, message);
        }
        else if (operation == 3)
        {
            operation_three(msg_queue_id, seq_num, message);
        }
        else if (operation == 4)
        {
            operation_four(msg_queue_id, seq_num, message);
        }
        else
        {
            printf("Invalid Input. Please try again.\n");
        }
    }

    return 0;
}
