#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <getopt.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/mman.h>


#define CHUNK_SIZE 4096 
#define SIZE_PLACEHOLDER 900000 


typedef struct Task
{
    unsigned char *data;
    int index;
    size_t size;
} Task;


size_t head = 0;
size_t tail = 0;
Task tq[900000];
size_t task_count = 0;
int total_task=0;
size_t *r_size;
unsigned char **resultQueue;
int count=0;

pthread_mutex_t mutex_queue = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond_get = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex_out = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t output_wait = PTHREAD_COND_INITIALIZER;

//**************************************************************************************/
// Counter
//**************************************************************************************/
void counter(){
    printf("[ %d ]\n",count);
    count++;
}


//**************************************************************************************/
// 1) Enqueue Task
//**************************************************************************************/
void submit_task(Task c){
    pthread_mutex_lock(&mutex_queue);
    tq[tail] = c;
    task_count++;
    total_task++;
    tail++;
    pthread_cond_signal(&cond_get);
    pthread_mutex_unlock(&mutex_queue);
}

//****************************************************************************************/
// 2) Encoder [Multi-threads]
//****************************************************************************************/
void *Encoder_m(void *arg){

   
    unsigned char **rq = ((unsigned char ***)arg)[0];
    size_t *rq_size = ((size_t **)arg)[1];

    while (1)
    {
       
         pthread_mutex_lock(&mutex_queue);
        while (task_count == 0)
         {
             pthread_cond_wait(&cond_get, &mutex_queue);
         }
         Task t = tq[head++];
         task_count--;
        pthread_mutex_unlock(&mutex_queue);

        if (t.data==NULL)
        {
            break;
        }

        size_t s;
        
         unsigned char *output = malloc(t.size * 2);
         unsigned char current_char = t.data[0];
         unsigned char count = 1;
             size_t index = 0;

        for (size_t i = 1; i < t.size; i++)
          {
            if (t.data[i] == current_char)
            {
               count++;
            }
           else
           {
               output[index++] = current_char;
               output[index++] = count;
               current_char = t.data[i];
                count = 1;
          }
         }

        output[index++] = current_char;
        output[index++] = count;
        s = index;
        
         pthread_mutex_lock(&mutex_out);
        rq[t.index] = output;
        rq_size[t.index] = s;
      
        pthread_cond_signal(&output_wait);
        pthread_mutex_unlock(&mutex_out);

        free(t.data);
    }
    return NULL;
}

//**************************************************************************************/
// 3) Get Task
//**************************************************************************************/

unsigned char* get_result(int index, size_t* out_size) {
    pthread_mutex_lock(&mutex_out);
    
    while (resultQueue[index] == NULL) {
        pthread_cond_wait(&output_wait, &mutex_out);
    }
    if(resultQueue[index] == NULL && count==total_task){

          pthread_mutex_unlock(&mutex_out);
        return NULL; 
    }

    unsigned char* result = resultQueue[index];
    *out_size = r_size[index];
    
    pthread_mutex_unlock(&mutex_out);
    
    count++;
    return result;
}

//**************************************************************************************/
// 3) MAIN
//**************************************************************************************/
int main(int argc, char *argv[])
{
   int threadNum = 1; 
    int f_start = 1;  


    for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-j") == 0 && i + 1 < argc) {
        threadNum = atoi(argv[++i]);  
    } else {
        f_start = i;  
        break;
    }
    }

    char **file = &argv[f_start];
    resultQueue = calloc(SIZE_PLACEHOLDER, sizeof(unsigned char *));
   
    void *parg[2];
    r_size = malloc(SIZE_PLACEHOLDER * sizeof(size_t));
    parg[0]=resultQueue;
    parg[1]=r_size;

    //Thread Pool
     pthread_t *threadPool = malloc(threadNum * sizeof(pthread_t));
        for (int i = 0; i < threadNum; i++) {
            pthread_create(&threadPool[i], NULL, Encoder_m, parg);
        }
    // Submit Task
    int id = 0;
    int file_count = argc - f_start;
    for (int i = 0; i < file_count; i++)
    {
        int fd = open(file[i], O_RDONLY);
       
        struct stat sb;
        if (fstat(fd, &sb) == -1)
        {
            close(fd);
            continue;
        }

        size_t f_size = sb.st_size;
        char *m = mmap(NULL, f_size, PROT_READ, MAP_PRIVATE, fd, 0);
        

        for (size_t i = 0; i < f_size; i += CHUNK_SIZE)
        {
            size_t remain = f_size - i;
          size_t length;
          if (remain < CHUNK_SIZE) {
             length = remain; 
          } else {
             length = CHUNK_SIZE; 
          }
            unsigned char *d_cup;
            int out_size = SIZE_PLACEHOLDER;

            d_cup = malloc(length);
            memcpy(d_cup, m+ i, length);

            if (id >= out_size)
            {
                out_size *= 2;
                resultQueue = realloc(resultQueue, out_size * sizeof(unsigned char *));
                r_size = realloc(r_size, out_size * sizeof(size_t));
                
            }

            Task task;
            task.index=id;
            task.data=d_cup;
            task.size=length;
            id++;
            submit_task(task);
        }

       
        close(fd);
    }
    
    Task sig_task;
    sig_task.data = NULL;
    sig_task.size = 0;
    sig_task.index = -1;
    for (int i = 0; i < threadNum; i++)
    {
         submit_task(sig_task);
    }
   
    int index = 1;
    unsigned char cup = 0;
    unsigned char count = 0;

    for (int i = 0; i < id; i++)
    {
        size_t size;
        unsigned char* result = get_result(i, &size);
         if (result == NULL) {
              break; 
         }
             unsigned char cup_c;
            unsigned char count_c;
        for (size_t j = 0; j < size; j += 2){
            cup_c = result[j];
            count_c = result[j + 1];
            if (index == 0 && cup_c == cup){
                count += count_c;
            }else if(index==0){
                unsigned char output[2];
                output[0] = cup;    
                output[1] = count; 

                ssize_t bytes_written = write(1, output, 2);
                if (bytes_written == -1) {
                exit(0);
                }

                    cup = cup_c;
                    count = count_c;
                  index = 0;
            }else{
            
                cup = cup_c;
                count = count_c;
                index = 0;
            }
        }
        free(result);
    }

    if (index==0){
         unsigned char output[2];
                output[0] = cup;    
                output[1] = count; 

                ssize_t bytes_written = write(1, output, 2);
                if (bytes_written == -1) {
                exit(0);
                }
    }

    for (int i = 0; i < threadNum; i++)
    {
        pthread_join(threadPool[i], NULL);
       
    }

        free(threadPool);
         free(r_size);
      free(resultQueue);
    

    pthread_mutex_destroy(&mutex_queue);
    pthread_mutex_destroy(&mutex_out);
    pthread_cond_destroy(&output_wait);
     pthread_cond_destroy(&cond_get);

    return 0;
}