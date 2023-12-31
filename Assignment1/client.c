/**
 * @file client.c
 * @author Divyateja Pasupuleti
 * @brief
 * @version 0.1
 * @date 2023-09-20
 *
 * @copyright Copyright (c) 2023
 * POSIX-compliant C program client.c
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <limits.h>

#define MESSAGE_LENGTH 100
#define SERVER_RECEIVES_ON_CHANNEL 1

/**
 * @brief The buffer structure for the message queue
 * NOTE: Here operation = r would mean that we are getting response from server
 */
struct data
{
    char message[MESSAGE_LENGTH];
    char operation;
    long client_id;
};

struct msg_buffer
{
    long msg_type;
    struct data data;
};

/**
Here we use data struct which keeps track of which operation is being performed. 1 stands for ping,
2 stands for file search, 3 stands for within file search and 4 starts of cleanup. Here the important
part is r. r stands for reply. When the server is replying to a client it uses r to ensure it doesn't
get mixed up by anything else. We also faced major issues while trying to use wait() since forgetting
wait leads to race conditions and it calling itself for an infinite number of times.
*/

/**
 * @brief The function to contact the Ping Server
 * In this function, the client process will send a message to the Ping Server
 * and wait for a response from the Ping Server. We will set operation to 1
 * to indicate that we are contacting the Ping Server and set msg_type to the
 * client id
 */
void client_ping(int msg_queue_id, int client_id, struct msg_buffer msg_buf)
{
    printf("[Client: Ping] Sending message to the Ping Server...\n");

    msg_buf.data.message[0] = 'H';
    msg_buf.data.message[1] = 'i';
    msg_buf.data.message[2] = '\0';

    msg_buf.msg_type = SERVER_RECEIVES_ON_CHANNEL;
    msg_buf.data.client_id = client_id;
    msg_buf.data.operation = '1';

    if (msgsnd(msg_queue_id, &msg_buf, sizeof(msg_buf.data), 0) == -1)
    {
        perror("[Client: Ping] Message could not be sent, please try again");
        exit(EXIT_FAILURE);
    }
    else
    {
        while (1)
        {
            if (msgrcv(msg_queue_id, &msg_buf, sizeof(msg_buf.data), client_id, 0) == -1)
            {
                perror("[Client: Ping] Error while receiving message from the Ping Server");
            }
            else
            {
                // Logging for debugging
                // printf("[Client: Ping] Message recieved from the Ping Server %ld: %s using %c\n", msg_buf.msg_type, msg_buf.data.message, msg_buf.data.operation);

                if (msg_buf.msg_type == client_id && msg_buf.data.operation == 'r')
                {
                    printf("[Client: Ping] Message received from the Ping Server: %s\n", msg_buf.data.message);
                    return;
                }
                else
                {
                    // push the message back to the queue
                    if (msgsnd(msg_queue_id, &msg_buf, MESSAGE_LENGTH, 0) == -1)
                    {
                        perror("[Client: Ping] Message could not be sent, please try again");
                    }
                }
            }
        }
    }
}

/**
 * @brief The function to contact the File Search Server.
 * Send the name of the relevant file and waits for reposnse from server
 *
 */
void client_file_search(int msg_queue_id, int client_id, struct msg_buffer msg_buf)
{
    printf("Enter the filename: ");
    scanf("%s", msg_buf.data.message);

    msg_buf.msg_type = SERVER_RECEIVES_ON_CHANNEL;
    msg_buf.data.client_id = client_id;
    msg_buf.data.operation = '2';

    if (msgsnd(msg_queue_id, &msg_buf, sizeof(msg_buf.data), 0) == -1)
    {
        perror("[Client: File Search] Message could not be sent, please try again");
        exit(EXIT_FAILURE);
    }
    else
    {
        while (1)
        {
            if (msgrcv(msg_queue_id, &msg_buf, sizeof(msg_buf.data), client_id, 0) == -1)
            {
                perror("[Client: File Search] Error while receiving message from the files search server");
            }
            else
            {
                // Logging for debugging
                // printf("[Client: File Search] Some message recieved from the files search server %ld: %s using %c\n", msg_buf.msg_type, msg_buf.data.message, msg_buf.data.operation);

                if (msg_buf.msg_type == client_id && msg_buf.data.operation == 'r')
                {
                    printf("[Client: File Search] Correct message received from the files search server: %s\n", msg_buf.data.message);
                    return;
                }
                else
                {
                    // push the message back to the queue
                    if (msgsnd(msg_queue_id, &msg_buf, MESSAGE_LENGTH, 0) == -1)
                    {
                        perror("[Client: File Search] Incorrect message couldn't be put back into queue");
                    }
                }
            }
        }
    }
}

/**
 * @brief The function to contact the File Word Count Server
 *
 */
void client_word_count(int msg_queue_id, int client_id, struct msg_buffer msg_buf)
{
    printf("Enter the filename: ");
    scanf("%s", msg_buf.data.message);

    msg_buf.msg_type = SERVER_RECEIVES_ON_CHANNEL;
    msg_buf.data.client_id = client_id;
    msg_buf.data.operation = '3';

    if (msgsnd(msg_queue_id, &msg_buf, sizeof(msg_buf.data), 0) == -1) // send message to server
    {
        printf("[Client: Word Count] Message could not be sent, please try again\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        while (1)
        {
            if (msgrcv(msg_queue_id, &msg_buf, sizeof(msg_buf.data), client_id, 0) == -1) // receive message from server
            {
                perror("[Client: Word Count] Error while receiving message from the files word count server");
            }
            else
            {
                // Logging for debugging
                // printf("[Client] Some message recieved from the files search server %ld: %s using %c\n", msg_buf.msg_type, msg_buf.data.message, msg_buf.data.operation);

                if (msg_buf.msg_type == client_id && msg_buf.data.operation == 'r') // check if message has reached right client, intended op type
                {
                    printf("[Client: Word Count] Correct message received from the files word count server: %s\n", msg_buf.data.message);
                    return;
                }
                else
                {
                    // push the message back to the queue
                    if (msgsnd(msg_queue_id, &msg_buf, MESSAGE_LENGTH, 0) == -1)
                    {
                        perror("[Client] Incorrect message couldn't be put back into queue");
                    }
                }
            }
        }
    }
}

/**
 * @brief The function to exit the client
 *
 */
void client_exit()
{
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
    printf("Initializing Client...\n");

    key_t key;
    int msg_queue_id;
    struct msg_buffer message;

    // Generate key for the message queue
    while ((key = ftok(".", 'B')) == -1)
    {
        perror("Error while generating key of the file");
        exit(EXIT_FAILURE);
    }

    // Connect to the messsage queue
    while ((msg_queue_id = msgget(key, 0644)) == -1)
    {
        perror("Error while connecting with Message Queue");
        exit(EXIT_FAILURE);
    }

    printf("Successfully connected to the Message Queue %d %d\n", key, msg_queue_id);

    // When a client process is run, it will ask the user to enter a positive integer as its client-id
    int client_id;
    printf("Enter Client-ID: ");
    scanf("%d", &client_id);

    if (client_id == 0)
    {
        printf("Client ID cannot be 0\n");
        exit(EXIT_FAILURE);
    }

    // This is being done to ensure that the client_id is unique and cannot be 1
    client_id += 1;

    // Display the menu
    while (1)
    {
        printf("\n");
        printf("Choose from one of the options below: \n");
        printf("1. Enter 1 to contact the Ping Server\n");
        printf("2. Enter 2 to contact the File Search Server\n");
        printf("3. Enter 3 to contact the File Word Count Server\n");
        printf("4. Enter 4 if this Client wishes to exit\nInput: ");

        int input;
        scanf("%d", &input);
        printf("\nInput given: %d\n", input);

        if (input == 1)
        {
            client_ping(msg_queue_id, client_id, message);
        }
        else if (input == 2)
        {
            client_file_search(msg_queue_id, client_id, message);
        }
        else if (input == 3)
        {
            client_word_count(msg_queue_id, client_id, message);
        }
        else if (input == 4)
        {
            exit(EXIT_SUCCESS);
        }
        else
        {
            printf("Invalid Input. Please try again.\n");
        }
    }

    return 0;
}
