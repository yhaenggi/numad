/*

numad - NUMA Daemon to automatically bind processes to NUMA nodes
Copyright (C) 2012 Bill Gray (bgray@redhat.com), Red Hat Inc

numad is free software; you can redistribute it and/or modify it under the
terms of the GNU Lesser General Public License as published by the Free
Software Foundation; version 2.1.

numad is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
details.

You should find a copy of v2.1 of the GNU Lesser General Public License
somewhere on your Linux system; if not, write to the Free Software Foundation,
Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

*/ 


// Compile with: gcc -std=gnu99 -g -Wall -pthread -o numad numad.c -lrt -lm


#define _GNU_SOURCE

#include <assert.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <values.h>

#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/syslog.h>
#include <sys/time.h>
#include <sys/types.h>

#include <asm/unistd.h>


#define VERSION_STRING "20150602"


#define VAR_RUN_FILE "/var/run/numad.pid"
#define VAR_LOG_FILE "/var/log/numad.log"

#define KILOBYTE (1024)
#define MEGABYTE (1024 * 1024)

#define FNAME_SIZE 192
#define BUF_SIZE 1024
#define BIG_BUF_SIZE 4096

// The ONE_HUNDRED factor is used to scale time and CPU usage units.
// Several CPU quantities are measured in percents of a CPU; and
// several time values are counted in hundreths of a second.
#define ONE_HUNDRED 100


#define MIN_INTERVAL  5
#define MAX_INTERVAL 15
#define CPU_THRESHOLD     50
#define MEMORY_THRESHOLD 300
#define DEFAULT_HTT_PERCENT 20
#define DEFAULT_THP_SCAN_SLEEP_MS 1000
#define DEFAULT_UTILIZATION_PERCENT 85
#define DEFAULT_MEMLOCALITY_PERCENT 90


#define CONVERT_DIGITS_TO_NUM(p, n) \
    n = *p++ - '0'; \
    while (isdigit(*p)) { \
        n *= 10; \
        n += (*p++ - '0'); \
    }


int num_cpus = 0;
int num_nodes = 0;
int threads_per_core = 0;
uint64_t page_size_in_bytes = 0;
uint64_t huge_page_size_in_bytes = 0;

int min_interval = MIN_INTERVAL;
int max_interval = MAX_INTERVAL;
int htt_percent = DEFAULT_HTT_PERCENT;
int thp_scan_sleep_ms = DEFAULT_THP_SCAN_SLEEP_MS;
int target_utilization  = DEFAULT_UTILIZATION_PERCENT;
int target_memlocality  = DEFAULT_MEMLOCALITY_PERCENT;
int scan_all_processes = 1;
int keep_interleaved_memory = 0;
int use_inactive_file_cache = 1;

pthread_mutex_t pid_list_mutex;
pthread_mutex_t node_info_mutex;
long sum_CPUs_total = 0;
int requested_mbs = 0;
int requested_cpus = 0;
int got_sighup = 0;
int got_sigterm = 0;
int got_sigquit = 0;

void sig_handler(int signum) { 
    switch (signum) {
        case SIGHUP:  got_sighup  = 1; break;
        case SIGTERM: got_sigterm = 1; break;
        case SIGQUIT: got_sigquit = 1; break;
    }
}



FILE *log_fs = NULL;
int log_level = LOG_NOTICE;

void numad_log(int level, const char *fmt, ...) {
    if (level > log_level) {
        return;
        // Logging levels (from sys/syslog.h)
        //     #define LOG_EMERG       0       /* system is unusable */
        //     #define LOG_ALERT       1       /* action must be taken immediately */
        //     #define LOG_CRIT        2       /* critical conditions */
        //     #define LOG_ERR         3       /* error conditions */
        //     #define LOG_WARNING     4       /* warning conditions */
        //     #define LOG_NOTICE      5       /* normal but significant condition */
        //     #define LOG_INFO        6       /* informational */
        //     #define LOG_DEBUG       7       /* debug-level messages */
    }
    char buf[BUF_SIZE];
    time_t ts = time(NULL);
    strncpy(buf, ctime(&ts), sizeof(buf));
    char *p = &buf[strlen(buf) - 1];
    *p++ = ':';
    *p++ = ' ';
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(p, BUF_SIZE, fmt, ap);
    va_end(ap);
    fprintf(log_fs, "%s", buf);
    fflush(log_fs);
}

void open_log_file() {
    log_fs = fopen(VAR_LOG_FILE, "a");
    if (log_fs == NULL) {
        log_fs = stderr;
        numad_log(LOG_ERR, "Cannot open numad log file (errno: %d) -- using stderr\n", errno);
    }
}


void close_log_file() {
    if (log_fs != NULL) {
        if (log_fs != stderr) {
            fclose(log_fs);
        }
        log_fs = NULL;
    }
}



#define MSG_BODY_TEXT_SIZE 96

typedef struct msg_body {
    long src_pid;
    long cmd;
    long arg1;
    long arg2;
    char text[MSG_BODY_TEXT_SIZE];
} msg_body_t, *msg_body_p;

typedef struct msg {
    long dst_pid;  // msg mtype is dest PID
    msg_body_t body;
} msg_t, *msg_p;

int msg_qid;

void flush_msg_queue() {
    msg_t msg;
    do {
        msgrcv(msg_qid, &msg, sizeof(msg_body_t), 0, IPC_NOWAIT);
    } while (errno != ENOMSG);
}

void init_msg_queue() {
    key_t msg_key = 0xdeadbeef;
    int msg_flg = 0660 | IPC_CREAT;
    msg_qid = msgget(msg_key, msg_flg);
    if (msg_qid < 0) {
        numad_log(LOG_CRIT, "msgget failed\n");
        exit(EXIT_FAILURE);
    }
    flush_msg_queue();
}

void recv_msg(msg_p m) {
    if (msgrcv(msg_qid, m, sizeof(msg_body_t), getpid(), 0) < 0) {
        numad_log(LOG_CRIT, "msgrcv failed\n");
        exit(EXIT_FAILURE);
    }
    // printf("Received: >>%s<< from process %d\n", m->body.text, m->body.src_pid);
}

void send_msg(long dst_pid, long cmd, long arg1, long arg2, char *s) {
    msg_t msg;
    msg.dst_pid = dst_pid;
    msg.body.src_pid = getpid();
    msg.body.cmd = cmd;
    msg.body.arg1 = arg1;
    msg.body.arg2 = arg2;
    int s_len = strlen(s);
    if (s_len >= MSG_BODY_TEXT_SIZE) {
        numad_log(LOG_CRIT, "msgsnd text too big\n");
        exit(EXIT_FAILURE);
    }
    strcpy(msg.body.text, s);
    size_t m_len = sizeof(msg_body_t) - MSG_BODY_TEXT_SIZE + s_len + 1;
    if (msgsnd(msg_qid, &msg, m_len, IPC_NOWAIT) < 0) {
        numad_log(LOG_CRIT, "msgsnd failed\n");
        exit(EXIT_FAILURE);
    }
    // printf("Sent: >>%s<< to process %d\n", msg.body.text, msg.dst_pid);
}



typedef struct id_list {
    // Use CPU_SET(3) <sched.h> bitmasks,
    // but bundle size and pointer together
    // and genericize for both CPU and Node IDs
    cpu_set_t *set_p; 
    size_t bytes;
} id_list_t, *id_list_p;

#define ID_LIST_SET_P(list_p) (list_p->set_p)
#define ID_LIST_BYTES(list_p) (list_p->bytes)

#define INIT_ID_LIST(list_p, num_elements) \
    list_p = malloc(sizeof(id_list_t)); \
    if (list_p == NULL) { numad_log(LOG_CRIT, "INIT_ID_LIST malloc failed\n"); exit(EXIT_FAILURE); } \
    list_p->set_p = CPU_ALLOC(num_elements); \
    if (list_p->set_p == NULL) { numad_log(LOG_CRIT, "CPU_ALLOC failed\n"); exit(EXIT_FAILURE); } \
    list_p->bytes = CPU_ALLOC_SIZE(num_elements);

#define CLEAR_CPU_LIST(list_p) \
    if (list_p == NULL) { \
        INIT_ID_LIST(list_p, num_cpus); \
    } \
    CPU_ZERO_S(list_p->bytes, list_p->set_p)

#define CLEAR_NODE_LIST(list_p) \
    if (list_p == NULL) { \
        INIT_ID_LIST(list_p, num_nodes); \
    } \
    CPU_ZERO_S(list_p->bytes, list_p->set_p)

#define FREE_LIST(list_p) \
    if (list_p != NULL) { \
        if (list_p->set_p != NULL) { CPU_FREE(list_p->set_p); } \
        free(list_p); \
        list_p = NULL; \
    }

#define COPY_LIST(orig_list_p, copy_list_p) \
    memcpy(copy_list_p->set_p, orig_list_p->set_p, orig_list_p->bytes)

#define NUM_IDS_IN_LIST(list_p)     CPU_COUNT_S(list_p->bytes, list_p->set_p)
#define ADD_ID_TO_LIST(k, list_p)  CPU_SET_S(k, list_p->bytes, list_p->set_p)
#define CLR_ID_IN_LIST(k, list_p)  CPU_CLR_S(k, list_p->bytes, list_p->set_p)
#define ID_IS_IN_LIST(k, list_p) CPU_ISSET_S(k, list_p->bytes, list_p->set_p)

#define           EQUAL_LISTS(list_1_p, list_2_p) CPU_EQUAL_S(list_1_p->bytes,                    list_1_p->set_p, list_2_p->set_p)
#define AND_LISTS(and_list_p, list_1_p, list_2_p) CPU_AND_S(and_list_p->bytes, and_list_p->set_p, list_1_p->set_p, list_2_p->set_p)
#define  OR_LISTS( or_list_p, list_1_p, list_2_p)  CPU_OR_S( or_list_p->bytes,  or_list_p->set_p, list_1_p->set_p, list_2_p->set_p)
#define XOR_LISTS(xor_list_p, list_1_p, list_2_p) CPU_XOR_S(xor_list_p->bytes, xor_list_p->set_p, list_1_p->set_p, list_2_p->set_p)

int negate_cpu_list(id_list_p list_p) {
    if (list_p == NULL) {
        numad_log(LOG_CRIT, "Cannot negate a NULL list\n");
        exit(EXIT_FAILURE);
    }
    if (num_cpus < 1) {
        numad_log(LOG_CRIT, "No CPUs to negate in list!\n");
        exit(EXIT_FAILURE);
    }
    for (int ix = 0;  (ix < num_cpus);  ix++) {
        if (ID_IS_IN_LIST(ix, list_p)) {
            CLR_ID_IN_LIST(ix, list_p);
        } else {
            ADD_ID_TO_LIST(ix, list_p);
        }
    }
    return NUM_IDS_IN_LIST(list_p);
}

int add_ids_to_list_from_str(id_list_p list_p, char *s) {
    if (list_p == NULL) {
        numad_log(LOG_CRIT, "Cannot add to NULL list\n");
        exit(EXIT_FAILURE);
    }
    if ((s == NULL) || (strlen(s) == 0)) {
        goto return_list;
    }
    int in_range = 0;
    int next_id = 0;
    for (;;) {
        // skip over non-digits
        while (!isdigit(*s)) {
            if ((*s == '\n') || (*s == '\0')) {
                goto return_list;
            }
            if (*s++ == '-') {
                in_range = 1;
            }
        }
        int id;
        CONVERT_DIGITS_TO_NUM(s, id);
        if (!in_range) {
            next_id = id;
        }
        for (; (next_id <= id); next_id++) {
            ADD_ID_TO_LIST(next_id, list_p);
        }
        in_range = 0;
    }
return_list:
    return NUM_IDS_IN_LIST(list_p);
}

int str_from_id_list(char *str_p, int str_size, id_list_p list_p) {
    char *p = str_p;
    if ((p == NULL) || (str_size < 3)) {
        numad_log(LOG_CRIT, "Bad string for ID listing\n");
        exit(EXIT_FAILURE);
    }
    int n;
    if ((list_p == NULL) || ((n = NUM_IDS_IN_LIST(list_p)) == 0)) {
        goto terminate_string;
    }
    int id_range_start = -1;
    for (int id = 0;  ;  id++) {
        int id_in_list = (ID_IS_IN_LIST(id, list_p) != 0);
        if ((id_in_list) && (id_range_start < 0)) {
            id_range_start = id; // beginning an ID range
        } else if ((!id_in_list) && (id_range_start >= 0)) {
            // convert the range that just ended...
            p += snprintf(p, (str_p + str_size - p - 1), "%d", id_range_start);
            if (id - id_range_start > 1) {
                *p++ = '-';
                p += snprintf(p, (str_p + str_size - p - 1), "%d", (id - 1));
            } 
            *p++ = ',';
            id_range_start = -1; // no longer in a range
            if (n <= 0) { break; } // exit only after finishing a range
        }
        n -= id_in_list;
    }
    p -= 1; // eliminate trailing ','
terminate_string:
    *p = '\0';
    return (p - str_p);
}



typedef struct node_data {
    uint64_t node_id;
    uint64_t MBs_total;
    uint64_t MBs_free;
    uint64_t CPUs_total; // scaled * ONE_HUNDRED
    uint64_t CPUs_free;  // scaled * ONE_HUNDRED
    uint64_t magnitude;  // hack: MBs * CPUs
    uint8_t *distance;
    id_list_p cpu_list_p; 
} node_data_t, *node_data_p;
node_data_p node = NULL;

int min_node_CPUs_free_ix = -1;
int min_node_MBs_free_ix = -1;
long min_node_CPUs_free = MAXINT;
long min_node_MBs_free = MAXINT;
long max_node_CPUs_free = 0;
long max_node_MBs_free = 0;
long avg_node_CPUs_free = 0;
long avg_node_MBs_free = 0;
double stddev_node_CPUs_free = 0.0;
double stddev_node_MBs_free = 0.0;



// RING_BUF_SIZE must be a power of two
#define RING_BUF_SIZE 8

#define PROCESS_FLAG_INTERLEAVED (1 << 0)

typedef struct process_data {
    int pid;
    unsigned int flags;
    uint64_t data_time_stamp; // hundredths of seconds
    uint64_t bind_time_stamp;
    uint64_t num_threads;
    uint64_t MBs_size;
    uint64_t MBs_used;
    uint64_t cpu_util;
    uint64_t CPUs_used;  // scaled * ONE_HUNDRED
    uint64_t CPUs_used_ring_buf[RING_BUF_SIZE];
    int ring_buf_ix;
    char *comm;
    id_list_p node_list_p;
    uint64_t *process_MBs;
} process_data_t, *process_data_p;



// Hash table size must always be a power of two
#define MIN_PROCESS_HASH_TABLE_SIZE 16
int process_hash_table_size = 0;
int process_hash_collisions = 0;
process_data_p process_hash_table = NULL;

int process_hash_ix(int pid) {
    unsigned ix = pid;
    ix *= 717;
    ix >>= 8;
    ix &= (process_hash_table_size - 1);
    return ix;
}

int process_hash_lookup(int pid) {
    int ix = process_hash_ix(pid);
    int starting_ix = ix;
    while (process_hash_table[ix].pid) {
        // Assumes table with some blank entries...
        if (pid == process_hash_table[ix].pid) {
            return ix;  // found it
        }
        ix += 1;
        ix &= (process_hash_table_size - 1);
        if (ix == starting_ix) {
            // Table full and pid not found.
            // This "should never happen"...
            break;
        }
    }
    return -1;
}

int process_hash_insert(int pid) {
    // This reserves the hash table slot, but initializes only the pid field
    int ix = process_hash_ix(pid);
    int starting_ix = ix;
    while (process_hash_table[ix].pid) {
        if (pid == process_hash_table[ix].pid) {
            return ix;  // found it
        }
        process_hash_collisions += 1;
        ix += 1;
        ix &= (process_hash_table_size - 1);
        if (ix == starting_ix) {
            // This "should never happen"...
            numad_log(LOG_ERR, "Process hash table is full\n");
            return -1;
        }
    }
    process_hash_table[ix].pid = pid;
    return ix;
}

int process_hash_update(process_data_p newp) {
    // This updates hash table stats for processes we are monitoring. Only the
    // scalar resource consumption stats need to be updated here.
    int new_hash_table_entry = 1;
    int ix = process_hash_insert(newp->pid);
    if (ix >= 0) {
        process_data_p p = &process_hash_table[ix];
        if (p->data_time_stamp) {
            new_hash_table_entry = 0;
            p->ring_buf_ix += 1;
            p->ring_buf_ix &= (RING_BUF_SIZE - 1);
            uint64_t cpu_util_diff = newp->cpu_util  - p->cpu_util;
            uint64_t  time_diff = newp->data_time_stamp - p->data_time_stamp;
            p->CPUs_used_ring_buf[p->ring_buf_ix] = 100 * (cpu_util_diff) / time_diff;
            // Use largest CPU utilization currently in ring buffer
            uint64_t max_CPUs_used = p->CPUs_used_ring_buf[0];
            for (int ix = 1;  (ix < RING_BUF_SIZE);  ix++) {
                if (max_CPUs_used < p->CPUs_used_ring_buf[ix]) {
                    max_CPUs_used = p->CPUs_used_ring_buf[ix];
                }
            }
            p->CPUs_used = max_CPUs_used;
        }
        if ((!p->comm) || (strcmp(p->comm, newp->comm))) {
            if (p->comm) {
                free(p->comm);
            }
            p->comm = strdup(newp->comm);
        }
        p->MBs_size = newp->MBs_size;
        p->MBs_used = newp->MBs_used;
        p->cpu_util = newp->cpu_util;
        p->num_threads = newp->num_threads;
        p->data_time_stamp = newp->data_time_stamp;
    }
    return new_hash_table_entry;
}

void process_hash_clear_all_bind_time_stamps() {
    for (int ix = 0;  (ix < process_hash_table_size);  ix++) {
        process_hash_table[ix].bind_time_stamp = 0;
    }
}

int process_hash_rehash(int old_ix) {
    // Given the index of a table entry that would otherwise be orphaned by
    // process_hash_remove(), reinsert into table using PID from existing record.
    process_data_p op = &process_hash_table[old_ix];
    int new_ix = process_hash_insert(op->pid);
    if (new_ix >= 0) {
        // Copy old slot to new slot, and zero old slot
        process_data_p np = &process_hash_table[new_ix];
        memcpy(np, op, sizeof(process_data_t));
        memset(op,  0, sizeof(process_data_t));
    }
    return new_ix;
}

int process_hash_remove(int pid) {
    int ix = process_hash_lookup(pid);
    if (ix >= 0) {
        // remove the target
        process_data_p dp = &process_hash_table[ix];
        if (dp->comm) { free(dp->comm); }
        if (dp->process_MBs) { free(dp->process_MBs); }
        FREE_LIST(dp->node_list_p);
        memset(dp, 0, sizeof(process_data_t));
        // bubble up the collision chain and rehash if neeeded
        for (;;) {
            ix += 1;
            ix &= (process_hash_table_size - 1);
            if ((pid = process_hash_table[ix].pid) <= 0) {
                break;
            }
            if (process_hash_lookup(pid) < 0) {
                if (process_hash_rehash(ix) < 0) {
                    numad_log(LOG_ERR, "rehash fail\n");
                }
            }
        }
    }
    return ix;
}

void process_hash_table_expand() {
    // Save old table size and address
    int old_size = process_hash_table_size;
    process_data_p old_table = process_hash_table;
    // Double size of table and allocate new space
    if (old_size > 0) {
        process_hash_table_size *= 2;
    } else {
        process_hash_table_size = MIN_PROCESS_HASH_TABLE_SIZE;
    }
    numad_log(LOG_DEBUG, "Expanding hash table size: %d\n", process_hash_table_size);
    process_hash_table = malloc(process_hash_table_size * sizeof(process_data_t));
    if (process_hash_table == NULL) {
        numad_log(LOG_CRIT, "hash table malloc failed\n");
        exit(EXIT_FAILURE);
    }
    // Clear the new table, and copy valid entries from old table
    memset(process_hash_table, 0, process_hash_table_size * sizeof(process_data_t));
    for (int ix = 0;  (ix < old_size);  ix++) {
        process_data_p p = &old_table[ix];
        if (p->pid) {
            int new_table_ix = process_hash_insert(p->pid);
            memcpy(&process_hash_table[new_table_ix], p, sizeof(process_data_t));
        }
    }
    if (old_table != NULL) {
        free(old_table);
    }
}

void process_hash_table_dump() {
    for (int ix = 0;  (ix < process_hash_table_size);  ix++) {
        process_data_p p = &process_hash_table[ix];
        if (p->pid) {
            numad_log(LOG_DEBUG,
                "ix: %d  PID: %d %s  Thds: %d  CPU %ld  MBs: %ld/%ld Data TS: %ld  Bind TS: %ld\n",
                ix, p->pid, ((p->comm != NULL) ? p->comm : "(Null)"), p->num_threads,
                p->CPUs_used, p->MBs_used, p->MBs_size, p->data_time_stamp, p->bind_time_stamp);
            // FIXME: make this dump every field, but this is not even currently used
        }
    }
}

void process_hash_table_cleanup(uint64_t update_time) {
    int num_hash_entries_used = 0;
    for (int ix = 0;  (ix < process_hash_table_size);  ix++) {
        process_data_p p = &process_hash_table[ix];
        if (p->pid) {
            num_hash_entries_used += 1;
            if (p->data_time_stamp < update_time) {
                // Mark as old, and zero CPU utilization
                p->data_time_stamp = 0;
                p->CPUs_used = 0;
                // Check for dead pids and remove them...
                if ((kill(p->pid, 0) == -1) && (errno == ESRCH)) {
                    // Seems dead.  Forget this pid
                    process_hash_remove(p->pid);
                    num_hash_entries_used -= 1;
                }
            }
        }
    }
    // Keep hash table approximately half empty
    if ((num_hash_entries_used * 7) / 4 > process_hash_table_size) {
        process_hash_table_expand();
    }
}



typedef struct pid_list {
    long pid;
    struct pid_list* next;
} pid_list_t, *pid_list_p;

pid_list_p include_pid_list = NULL;
pid_list_p exclude_pid_list = NULL;

pid_list_p insert_pid_into_pid_list(pid_list_p list_ptr, long pid) {
    if (process_hash_table != NULL) {
        int hash_ix = process_hash_lookup(pid);
        if ((hash_ix >= 0) && (list_ptr == include_pid_list)) {
            // Clear interleaved flag, in case user wants it to be re-evaluated
            process_hash_table[hash_ix].flags &= ~PROCESS_FLAG_INTERLEAVED;
        }
    }
    // Check for duplicate pid first
    pid_list_p pid_ptr = list_ptr;
    while (pid_ptr != NULL) {
        if (pid_ptr->pid == pid) {
            // pid already in list
            return list_ptr;
        }
        pid_ptr = pid_ptr->next;
    }
    // pid not yet in list -- insert new node
    pid_ptr = malloc(sizeof(pid_list_t));
    if (pid_ptr == NULL) {
        numad_log(LOG_CRIT, "pid_list malloc failed\n");
        exit(EXIT_FAILURE);
    }
    pid_ptr->pid = pid;
    pid_ptr->next = list_ptr;
    list_ptr = pid_ptr;
    return list_ptr;
}

pid_list_p remove_pid_from_pid_list(pid_list_p list_ptr, long pid) {
    pid_list_p last_pid_ptr = NULL;
    pid_list_p pid_ptr = list_ptr;
    while (pid_ptr != NULL) {
        if (pid_ptr->pid == pid) {
            if (pid_ptr == list_ptr) {
                list_ptr = list_ptr->next;
                free(pid_ptr);
                pid_ptr = list_ptr;
                continue;
            } else {
                last_pid_ptr->next = pid_ptr->next;
                free(pid_ptr);
                pid_ptr = last_pid_ptr;
            }
        }
        last_pid_ptr = pid_ptr;
        pid_ptr = pid_ptr->next;
    }
    return list_ptr;
}



void shut_down_numad() {
    numad_log(LOG_NOTICE, "Shutting down numad\n");
    flush_msg_queue();
    unlink(VAR_RUN_FILE);
    close_log_file();
    exit(EXIT_SUCCESS);
}


void print_version_and_exit(char *prog_name) {
    fprintf(stdout, "%s version: %s: compiled %s\n", prog_name, VERSION_STRING, __DATE__);
    exit(EXIT_SUCCESS);
}


void print_usage_and_exit(char *prog_name) {
    fprintf(stderr, "Usage: %s <options> ...\n", prog_name);
    fprintf(stderr, "-C 1  to count inactive file cache as available memory (default 1)\n");
    fprintf(stderr, "-C 0  to count inactive file cache memory as unavailable (default 1)\n");
    fprintf(stderr, "-d for debug logging (same effect as '-l 7')\n");
    fprintf(stderr, "-h to print this usage info\n");
    fprintf(stderr, "-H <N> to set THP scan_sleep_ms (default %d)\n", DEFAULT_THP_SCAN_SLEEP_MS);
    fprintf(stderr, "-i [<MIN>:]<MAX> to specify interval seconds\n");
    fprintf(stderr, "-K 1  to keep interleaved memory spread across nodes (default 0)\n");
    fprintf(stderr, "-K 0  to merge interleaved memory to local NUMA nodes (default 0)\n");
    fprintf(stderr, "-l <N> to specify logging level (usually 5, 6, or 7 -- default 5)\n");
    fprintf(stderr, "-m <N> to specify memory locality target percent (default %d)\n", DEFAULT_MEMLOCALITY_PERCENT);
    fprintf(stderr, "-p <PID> to add PID to inclusion pid list\n");
    fprintf(stderr, "-r <PID> to remove PID from explicit pid lists\n");
    fprintf(stderr, "-R <CPU_LIST> to reserve some CPUs for non-numad use\n");
    fprintf(stderr, "-S 1  to scan all processes (default 1)\n");
    fprintf(stderr, "-S 0  to scan only explicit PID list processes (default 1)\n");
    fprintf(stderr, "-t <N> to specify thread / logical CPU valuation percent (default %d)\n", DEFAULT_HTT_PERCENT);
    fprintf(stderr, "-u <N> to specify utilization target percent (default %d)\n", DEFAULT_UTILIZATION_PERCENT);
    fprintf(stderr, "-v for verbose  (same effect as '-l 6')\n");
    fprintf(stderr, "-V to show version info\n");
    fprintf(stderr, "-w <CPUs>[:<MBs>] for NUMA node suggestions\n");
    fprintf(stderr, "-x <PID> to add PID to exclusion pid list\n");
    exit(EXIT_FAILURE);
}


void set_thp_scan_sleep_ms(int new_ms) {
    if (new_ms < 1) {
        // 0 means do not change the system default
        return;
    }
    char *thp_scan_fname = "/sys/kernel/mm/transparent_hugepage/khugepaged/scan_sleep_millisecs";
    int fd = open(thp_scan_fname, O_RDWR, 0);
    if (fd >= 0) {
        char buf[BUF_SIZE];
        int bytes = read(fd, buf, BUF_SIZE);
        if (bytes > 0) {
            buf[bytes] = '\0';
            int cur_ms;
            char *p = buf;
            CONVERT_DIGITS_TO_NUM(p, cur_ms);
            if (cur_ms != new_ms) {
                lseek(fd, 0, SEEK_SET);
                numad_log(LOG_NOTICE, "Changing THP scan time in %s from %d to %d ms.\n", thp_scan_fname, cur_ms, new_ms);
                sprintf(buf, "%d\n", new_ms);
                write(fd, buf, strlen(buf));
            }
        }
        close(fd);
    }
}

void check_prereqs(char *prog_name) {
    // Adjust kernel tunable to scan for THP more frequently...
    set_thp_scan_sleep_ms(thp_scan_sleep_ms);
}


int get_daemon_pid() {
    int fd = open(VAR_RUN_FILE, O_RDONLY, 0);
    if (fd < 0) {
        return 0;
    }
    char buf[BUF_SIZE];
    int bytes = read(fd, buf, BUF_SIZE);
    close(fd);
    if (bytes <= 0) {
        return 0;
    }
    int pid;
    char *p = buf;
    CONVERT_DIGITS_TO_NUM(p, pid);
    // Check run file pid still active
    char fname[FNAME_SIZE];
    snprintf(fname, FNAME_SIZE, "/proc/%d", pid);
    if (access(fname, F_OK) < 0) {
        if (errno == ENOENT) {
            numad_log(LOG_NOTICE, "Removing out-of-date numad run file because %s doesn't exist\n", fname);
            unlink(VAR_RUN_FILE);
        }
        return 0;
    }
    // Daemon must be running already.
    return pid; 
}

int register_numad_pid() {
    int pid;
    char buf[BUF_SIZE];
    int fd;
create_run_file:
    fd = open(VAR_RUN_FILE, O_RDWR|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (fd >= 0) {
        pid = getpid();
        sprintf(buf, "%d\n", pid);
        write(fd, buf, strlen(buf));
        close(fd);
        numad_log(LOG_NOTICE, "Registering numad version %s PID %d\n", VERSION_STRING, pid);
        return pid;
    }
    if (errno == EEXIST) {
        fd = open(VAR_RUN_FILE, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
        if (fd < 0) {
            goto fail_numad_run_file;
        }
        int bytes = read(fd, buf, BUF_SIZE);
        close(fd);
        if (bytes > 0) {
            char *p = buf;
            CONVERT_DIGITS_TO_NUM(p, pid);
            // Check pid in run file still active
            char fname[FNAME_SIZE];
            snprintf(fname, FNAME_SIZE, "/proc/%d", pid);
            if (access(fname, F_OK) < 0) {
                if (errno == ENOENT) {
                    // Assume run file is out-of-date...
                    numad_log(LOG_NOTICE, "Removing out-of-date numad run file because %s doesn't exist\n", fname);
                    unlink(VAR_RUN_FILE);
                    goto create_run_file;
                }
            }
            // Daemon must be running already.
            return pid; 
        }
    }
fail_numad_run_file:
    numad_log(LOG_CRIT, "Cannot open numad.pid file\n");
    exit(EXIT_FAILURE);
}


int count_set_bits_in_hex_list_file(char *fname) {
    int sum = 0;
    int fd = open(fname, O_RDONLY, 0);
    if (fd >= 0) {
        char buf[BUF_SIZE];
        int bytes = read(fd, buf, BUF_SIZE);
        close(fd);
        for (int ix = 0;  (ix < bytes);  ix++) {
            char c = tolower(buf[ix]);
            switch (c) {
                case '0'  : sum += 0; break;
                case '1'  : sum += 1; break;
                case '2'  : sum += 1; break;
                case '3'  : sum += 2; break;
                case '4'  : sum += 1; break;
                case '5'  : sum += 2; break;
                case '6'  : sum += 2; break;
                case '7'  : sum += 3; break;
                case '8'  : sum += 1; break;
                case '9'  : sum += 2; break;
                case 'a'  : sum += 2; break;
                case 'b'  : sum += 3; break;
                case 'c'  : sum += 2; break;
                case 'd'  : sum += 3; break;
                case 'e'  : sum += 3; break;
                case 'f'  : sum += 4; break;
                case ' '  : sum += 0; break;
                case ','  : sum += 0; break;
                case '\n' : sum += 0; break;
                default : numad_log(LOG_CRIT, "Unexpected character in list\n"); exit(EXIT_FAILURE);
            }
        }
    }
    return sum;
}


int get_num_cpus() {
    int n1 = sysconf(_SC_NPROCESSORS_CONF);
    int n2 = sysconf(_SC_NPROCESSORS_ONLN);
    if (n1 < n2) {
        n1 = n2;
    }
    if (n1 < 0) {
        numad_log(LOG_CRIT, "Cannot count number of processors\n");
        exit(EXIT_FAILURE);
    }
    return n1;
}


int get_num_kvm_vcpu_threads(int pid) {
    // Try to return the number of vCPU threads for this VM guest,
    // excluding the IO threads.  All failures return MAXINT.
    // FIXME: someday figure out some better way to do this...
    char fname[FNAME_SIZE];
    snprintf(fname, FNAME_SIZE, "/proc/%d/cmdline", pid);
    int fd = open(fname, O_RDONLY, 0);
    if (fd >= 0) {
        char buf[BUF_SIZE];
        int bytes = read(fd, buf, BUF_SIZE);
        close(fd);
        if (bytes > 0) {
            char *p = memmem(buf, bytes, "smp", 3);
            if (p != NULL) {
                while (!isdigit(*p) && (p - buf < bytes - 2)) {
                    p++;
                }
                if (isdigit(*p)) {
                    int vcpu_threads;
                    CONVERT_DIGITS_TO_NUM(p, vcpu_threads);
                    if ((vcpu_threads > 0) && (vcpu_threads <= num_cpus)) {
                        return vcpu_threads;
                    }
                }
            }
        }
    }
    return MAXINT;
}


uint64_t get_huge_page_size_in_bytes() {
    uint64_t huge_page_size = 0;;
    FILE *fs = fopen("/proc/meminfo", "r");
    if (!fs) {
        numad_log(LOG_CRIT, "Can't open /proc/meminfo\n");
        exit(EXIT_FAILURE);
    }
    char buf[BUF_SIZE];
    while (fgets(buf, BUF_SIZE, fs)) {
        if (!strncmp("Hugepagesize", buf, 12)) {
            char *p = &buf[12];
            while ((!isdigit(*p)) && (p < buf + BUF_SIZE)) {
                p++;
            }
            huge_page_size = atol(p);
            break;
        }
    }
    fclose(fs);
    return huge_page_size * KILOBYTE;
}


uint64_t get_time_stamp() {
    // Return time stamp in hundredths of a second
    struct timespec ts; 
    if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0) {
        numad_log(LOG_CRIT, "Cannot get clock_gettime()\n");
        exit(EXIT_FAILURE);
    }
    return (ts.tv_sec * ONE_HUNDRED) +
           (ts.tv_nsec / (1000000000 / ONE_HUNDRED));
}


static int name_starts_with_digit(const struct dirent *dptr) {
    return (isdigit(dptr->d_name[0]));
}



#define BITS_IN_LONG (CHAR_BIT * sizeof(unsigned long))
#define   SET_BIT(i,a)   (a)[(i) / BITS_IN_LONG] |=  (1u << ((i) % BITS_IN_LONG))
#define  TEST_BIT(i,a) (((a)[(i) / BITS_IN_LONG] &   (1u << ((i) % BITS_IN_LONG))) != 0)
#define CLEAR_BIT(i,a)   (a)[(i) / BITS_IN_LONG] &= ~(1u << ((i) % BITS_IN_LONG))

int bind_process_and_migrate_memory(process_data_p p) {
    uint64_t t0 = get_time_stamp();
    // Parameter p is a pointer to an element in the hash table
    if ((!p) || (p->pid < 1)) {
        numad_log(LOG_CRIT, "Bad PID to bind\n");
        exit(EXIT_FAILURE);
    }
    if (!p->node_list_p) {
        numad_log(LOG_CRIT, "Cannot bind to unspecified node(s)\n");
        exit(EXIT_FAILURE);
    }
    // Generate CPU list derived from target node list.
    static id_list_p cpu_bind_list_p;
    CLEAR_CPU_LIST(cpu_bind_list_p);
    int nodes = NUM_IDS_IN_LIST(p->node_list_p);
    int node_id = 0;
    while (nodes) {
        if (ID_IS_IN_LIST(node_id, p->node_list_p)) {
            OR_LISTS(cpu_bind_list_p, cpu_bind_list_p, node[node_id].cpu_list_p);
            nodes -= 1;
        }
        node_id += 1;
    }
    char fname[FNAME_SIZE];
    struct dirent **namelist;
    snprintf(fname, FNAME_SIZE, "/proc/%d/task", p->pid);
    int num_tasks = scandir(fname, &namelist, name_starts_with_digit, NULL);
    if (num_tasks <= 0) {
        numad_log(LOG_WARNING, "Could not scandir task list for PID: %d\n", p->pid);
        return 0;  // Assume the process terminated
    }
    // Set the affinity of each task in the process...
    for (int namelist_ix = 0;  (namelist_ix < num_tasks);  namelist_ix++) {
        int tid = atoi(namelist[namelist_ix]->d_name);
        int rc = sched_setaffinity(tid, ID_LIST_BYTES(cpu_bind_list_p), ID_LIST_SET_P(cpu_bind_list_p));
        if (rc < 0) {
            // Check errno
            if (errno == ESRCH) {
                numad_log(LOG_WARNING, "Tried to move PID %d, TID %d, but it apparently went away.\n", p->pid, tid);
            }
            numad_log(LOG_ERR, "Bad sched_setaffinity() on PID %d, TID %d -- errno: %d\n", p->pid, tid, errno);
        }
        free(namelist[namelist_ix]);
    }
    free(namelist);
    // Now move the memory to the target nodes....
    static unsigned long *dest_mask;
    static unsigned long *from_mask;
    static int allocated_bytes_in_masks;
    // Lie about num_nodes being one bigger because of kernel bug...
    int num_bytes_in_masks = (1 + ((num_nodes + 1) / BITS_IN_LONG)) * sizeof(unsigned long);
    if (allocated_bytes_in_masks < num_bytes_in_masks) {
        allocated_bytes_in_masks = num_bytes_in_masks;
        dest_mask = realloc(dest_mask, num_bytes_in_masks);
        from_mask = realloc(from_mask, num_bytes_in_masks);
        if ((dest_mask == NULL) || (from_mask == NULL)) {
            numad_log(LOG_CRIT, "bit mask malloc failed\n");
            exit(EXIT_FAILURE);
        }
    }
    // In an effort to put semi-balanced memory in each target node, move the
    // contents from the source node with the max amount of memory to the
    // destination node with the least amount of memory.  Repeat until done.
    int prev_from_node_id = -1;
    for (;;) {
        int min_dest_node_id = -1;
        int max_from_node_id = -1;
        for (int node_ix = 0;  (node_ix < num_nodes);  node_ix++) {
            node_id = node[node_ix].node_id;
            if (ID_IS_IN_LIST(node_id, p->node_list_p)) {
                if ((min_dest_node_id < 0) || (p->process_MBs[min_dest_node_id] >= p->process_MBs[node_id])) {
                    // The ">=" above is intentional, so we tend to move memory to higher numbered nodes
                    min_dest_node_id = node_id;
                }
            } else {
                if ((max_from_node_id < 0) || (p->process_MBs[max_from_node_id] < p->process_MBs[node_id])) {
                    max_from_node_id = node_id;
                }
            }
        }
        if ((p->process_MBs[max_from_node_id] == 0) || (max_from_node_id == prev_from_node_id)) {
            break;
        }
        memset(dest_mask, 0, num_bytes_in_masks);
        memset(from_mask, 0, num_bytes_in_masks);
        SET_BIT(max_from_node_id, from_mask);
        SET_BIT(min_dest_node_id, dest_mask);
        numad_log(LOG_DEBUG, "Moving memory from node: %d to node %d\n", max_from_node_id, min_dest_node_id);
        // Lie about num_nodes being one bigger because of kernel bug...
        int rc = syscall(__NR_migrate_pages, p->pid, num_nodes + 1, from_mask, dest_mask);
        if (rc > 2) {
            // rc == the number of pages that could not be moved.  
            // A couple pages not moving is probably not a problem, hence ignoring rc == 1 or 2.
            numad_log(LOG_WARNING, "Tried to move PID %d, but %d pages would not move.\n", p->pid, rc);
        } else if (rc < 0) {
            // Check errno
            if (errno == ESRCH) {
                numad_log(LOG_WARNING, "Tried to move PID %d, but it apparently went away.\n", p->pid);
                return 0;  // Assume the process terminated
            }
        }
        // Assume memory did move for current accounting purposes...
        p->process_MBs[min_dest_node_id] += p->process_MBs[max_from_node_id];
        p->process_MBs[max_from_node_id] = 0;
        prev_from_node_id = max_from_node_id;
    }
    // Check pid still active
    snprintf(fname, FNAME_SIZE, "/proc/%d", p->pid);
    if (access(fname, F_OK) < 0) {
        numad_log(LOG_WARNING, "Could not migrate pid %d.  Apparently it went away.\n", p->pid);
        return 0;
    } else {
        uint64_t t1 = get_time_stamp();
        p->bind_time_stamp = t1;
        char node_list_str[BUF_SIZE];
        str_from_id_list(node_list_str, BUF_SIZE, p->node_list_p);
        numad_log(LOG_NOTICE, "PID %d moved to node(s) %s in %d.%d seconds\n", p->pid, node_list_str, (t1-t0)/100, (t1-t0)%100);
        return 1;
    }
}



typedef struct cpu_data {
    uint64_t time_stamp;
    uint64_t *idle;
} cpu_data_t, *cpu_data_p;

cpu_data_t cpu_data_buf[2];  // Two sets, to calc deltas
int cur_cpu_data_buf = 0;

void update_cpu_data() {
    // Parse idle percents from CPU stats in /proc/stat cpu<N> lines
    static FILE *fs;
    if (fs != NULL) {
        rewind(fs);
    } else {
        fs = fopen("/proc/stat", "r");
        if (!fs) {
            numad_log(LOG_CRIT, "Cannot get /proc/stat contents\n");
            exit(EXIT_FAILURE);
        }
        cpu_data_buf[0].idle = malloc(num_cpus * sizeof(uint64_t));
        cpu_data_buf[1].idle = malloc(num_cpus * sizeof(uint64_t));
        if ((cpu_data_buf[0].idle == NULL) || (cpu_data_buf[1].idle == NULL)) {
            numad_log(LOG_CRIT, "cpu_data_buf malloc failed\n");
            exit(EXIT_FAILURE);
        }
    }
    // Use the other cpu_data buffer...
    int new = 1 - cur_cpu_data_buf;
    // First get the current time stamp
    cpu_data_buf[new].time_stamp = get_time_stamp();
    // Now pull the idle stat from each cpu<N> line
    char buf[BUF_SIZE];
    while (fgets(buf, BUF_SIZE, fs)) {
        /* 
        * Lines are of the form:
        *
        * cpu<N> user nice system idle iowait irq softirq steal guest guest_nice
        *
        * # cat /proc/stat
        * cpu  11105906 0 78639 3359578423 24607 151679 322319 0 0 0
        * cpu0 190540 0 1071 52232942 39 7538 234039 0 0 0
        * cpu1 124519 0 50 52545188 0 1443 6267 0 0 0
        * cpu2 143133 0 452 52531440 36 1573 834 0 0 0
        * . . . . 
        */
        if ( (buf[0] == 'c') && (buf[1] == 'p') && (buf[2] == 'u') && (isdigit(buf[3])) ) {
            char *p = &buf[3];
            int cpu_id = *p++ - '0'; while (isdigit(*p)) { cpu_id *= 10; cpu_id += (*p++ - '0'); }
            while (!isdigit(*p)) { p++; } while (isdigit(*p)) { p++; }  // skip user
            while (!isdigit(*p)) { p++; } while (isdigit(*p)) { p++; }  // skip nice
            while (!isdigit(*p)) { p++; } while (isdigit(*p)) { p++; }  // skip system
            while (!isdigit(*p)) { p++; }
            uint64_t idle;
            CONVERT_DIGITS_TO_NUM(p, idle);
            cpu_data_buf[new].idle[cpu_id] = idle;
        }
    }
    cur_cpu_data_buf = new;
}

int node_and_digits(const struct dirent *dptr) {
    char *p = (char *)(dptr->d_name);
    if (*p++ != 'n') return 0;
    if (*p++ != 'o') return 0;
    if (*p++ != 'd') return 0;
    if (*p++ != 'e') return 0;
    do {
        if (!isdigit(*p++))
            return 0;
    } while (*p != '\0');
    return 1;
}


uint64_t node_info_time_stamp = 0;
id_list_p all_cpus_list_p = NULL;
id_list_p all_nodes_list_p = NULL;
id_list_p reserved_cpu_mask_list_p = NULL;
char *reserved_cpu_str = NULL;

void show_nodes() {
    fprintf(log_fs, "\n");
    numad_log(LOG_INFO, "Nodes: %d\n", num_nodes);
    fprintf(log_fs, "Min CPUs free: %ld, Max CPUs: %ld, Avg CPUs: %ld, StdDev: %lg\n", 
        min_node_CPUs_free, max_node_CPUs_free, avg_node_CPUs_free, stddev_node_CPUs_free);
    fprintf(log_fs, "Min MBs free: %ld, Max MBs: %ld, Avg MBs: %ld, StdDev: %lg\n", 
        min_node_MBs_free, max_node_MBs_free, avg_node_MBs_free, stddev_node_MBs_free);
    for (int ix = 0;  (ix < num_nodes);  ix++) {
        fprintf(log_fs, "Node %d: MBs_total %ld, MBs_free %6ld, CPUs_total %ld, CPUs_free %4ld,  Distance: ", 
            ix, node[ix].MBs_total, node[ix].MBs_free, node[ix].CPUs_total, node[ix].CPUs_free);
        for (int d = 0;  (d < num_nodes);  d++) {
            fprintf(log_fs, "%d ", node[ix].distance[d]);
        }
        char buf[BUF_SIZE];
        str_from_id_list(buf, BUF_SIZE, node[ix].cpu_list_p);
        fprintf(log_fs, " CPUs: %s\n", buf);
    }
    fflush(log_fs);
}

int update_nodes() {
    char fname[FNAME_SIZE];
    char buf[BIG_BUF_SIZE];
    // First, check to see if we should refresh basic node info that probably never changes...
    uint64_t time_stamp = get_time_stamp();
#define STATIC_NODE_INFO_DELAY (600 * ONE_HUNDRED)
    if ((num_nodes == 0) || (node_info_time_stamp + STATIC_NODE_INFO_DELAY < time_stamp)) {
        node_info_time_stamp = time_stamp;
        // Count directory names of the form: /sys/devices/system/node/node<N>
        struct dirent **namelist;
        int num_files = scandir ("/sys/devices/system/node", &namelist, node_and_digits, NULL);
        if (num_files < 1) {
            numad_log(LOG_CRIT, "Could not get NUMA node info\n");
            exit(EXIT_FAILURE);
        }
        int need_to_realloc = (num_files != num_nodes);
        if (need_to_realloc) {
            for (int ix = num_files;  (ix < num_nodes);  ix++) {
                // If new < old, free old node_data pointers
                free(node[ix].distance);
                FREE_LIST(node[ix].cpu_list_p);
            }
            node = realloc(node, (num_files * sizeof(node_data_t)));
            if (node == NULL) {
                numad_log(LOG_CRIT, "node realloc failed\n");
                exit(EXIT_FAILURE);
            }
            for (int ix = num_nodes;  (ix < num_files);  ix++) {
                // If new > old, nullify new node_data pointers
                node[ix].distance = NULL;
                node[ix].cpu_list_p = NULL;
            }
            num_nodes = num_files;
        }
        sum_CPUs_total = 0;
        CLEAR_CPU_LIST(all_cpus_list_p);
        CLEAR_NODE_LIST(all_nodes_list_p);
        // Figure out how many threads per core there are (for later discounting of hyper-threads)
        threads_per_core = count_set_bits_in_hex_list_file("/sys/devices/system/cpu/cpu0/topology/thread_siblings");
        if (threads_per_core < 1) {
            numad_log(LOG_CRIT, "Could not count threads per core\n");
            exit(EXIT_FAILURE);
        }
        // For each "node<N>" filename present, save <N> in node[ix].node_id
        // Note that the node id might not necessarily match the node ix.
        // Also populate the cpu lists and distance vectors for this node.
        for (int node_ix = 0;  (node_ix < num_nodes);  node_ix++) {
            int node_id;
            char *p = &namelist[node_ix]->d_name[4];
            CONVERT_DIGITS_TO_NUM(p, node_id);
            free(namelist[node_ix]);
            node[node_ix].node_id = node_id;
            ADD_ID_TO_LIST(node_id, all_nodes_list_p);
            // Get all the CPU IDs in this node...  Read lines from node<N>/cpulist
            // file, and set the corresponding bits in the node cpu list.
            snprintf(fname, FNAME_SIZE, "/sys/devices/system/node/node%d/cpulist", node_id);
            int fd = open(fname, O_RDONLY, 0);
            if ((fd >= 0) && (read(fd, buf, BIG_BUF_SIZE) > 0)) {
                buf[BIG_BUF_SIZE - 1] = '\0';
                // get cpulist from the cpulist string
                CLEAR_CPU_LIST(node[node_ix].cpu_list_p);
                int n = add_ids_to_list_from_str(node[node_ix].cpu_list_p, buf);
                if (reserved_cpu_str != NULL) {
                    AND_LISTS(node[node_ix].cpu_list_p, node[node_ix].cpu_list_p, reserved_cpu_mask_list_p);
                    n = NUM_IDS_IN_LIST(node[node_ix].cpu_list_p);
                }
                OR_LISTS(all_cpus_list_p, all_cpus_list_p, node[node_ix].cpu_list_p);
                // Calculate total CPUs, but possibly discount hyper-threads
                if ((threads_per_core == 1) || (htt_percent >= 100)) {
                    node[node_ix].CPUs_total = n * ONE_HUNDRED;
                } else {
                    n /= threads_per_core;
                    node[node_ix].CPUs_total = n * ONE_HUNDRED;
                    node[node_ix].CPUs_total += n * (threads_per_core - 1) * htt_percent;
                }
                sum_CPUs_total += node[node_ix].CPUs_total;
                close(fd);
            } else {
                numad_log(LOG_CRIT, "Could not get node cpu list\n");
                exit(EXIT_FAILURE);
            }
            // Get distance vector of ACPI SLIT data from node<N>/distance file
            if (need_to_realloc) {
                node[node_ix].distance = realloc(node[node_ix].distance, (num_nodes * sizeof(uint8_t)));
                if (node[node_ix].distance == NULL) {
                    numad_log(LOG_CRIT, "node distance realloc failed\n");
                    exit(EXIT_FAILURE);
                }
            }
            snprintf(fname, FNAME_SIZE, "/sys/devices/system/node/node%d/distance", node_id);
            fd = open(fname, O_RDONLY, 0);
            if ((fd >= 0) && (read(fd, buf, BIG_BUF_SIZE) > 0)) {
                int rnode = 0;
                for (char *p = buf;  (*p != '\n'); ) {
                    int lat;
                    CONVERT_DIGITS_TO_NUM(p, lat);
                    node[node_ix].distance[rnode++] = lat;
                    while (*p == ' ') { p++; }
                }
                close(fd);
            } else {
                numad_log(LOG_CRIT, "Could not get node distance data\n");
                exit(EXIT_FAILURE);
            }
        }
        free(namelist);
    }
    // Second, update the dynamic free memory and available CPU capacity
    while (cpu_data_buf[cur_cpu_data_buf].time_stamp + 7 >= time_stamp) {
        // Make sure at least 7/100 of a second has passed.
        // Otherwise sleep for 1/10 second.
	struct timespec ts = { 0, 100000000 }; 
	nanosleep(&ts, &ts);
	time_stamp = get_time_stamp();
    }
    update_cpu_data();
    max_node_MBs_free = 0;
    max_node_CPUs_free = 0;
    min_node_MBs_free = MAXINT;
    min_node_CPUs_free = MAXINT;
    uint64_t sum_of_node_MBs_free = 0;
    uint64_t sum_of_node_CPUs_free = 0;
    for (int node_ix = 0;  (node_ix < num_nodes);  node_ix++) {
        int node_id = node[node_ix].node_id;
        // Get available memory info from node<N>/meminfo file
        snprintf(fname, FNAME_SIZE, "/sys/devices/system/node/node%d/meminfo", node_id);
        int fd = open(fname, O_RDONLY, 0);
        if ((fd >= 0) && (read(fd, buf, BIG_BUF_SIZE) > 0)) {
            close(fd);
            uint64_t KB;
            buf[BIG_BUF_SIZE - 1] = '\0';
            char *p = strstr(buf, "MemTotal:");
            if (p != NULL) {
                p += 9;
            } else {
                numad_log(LOG_CRIT, "Could not get node MemTotal\n");
                exit(EXIT_FAILURE);
            }
            while (!isdigit(*p)) { p++; }
            CONVERT_DIGITS_TO_NUM(p, KB);
            node[node_ix].MBs_total = (KB / KILOBYTE);
            if (node[node_ix].MBs_total < 1) {
                // If a node has zero memory, remove it from the all_nodes_list...
                CLR_ID_IN_LIST(node_id, all_nodes_list_p);
            }
            p = strstr(p, "MemFree:");
            if (p != NULL) {
                p += 8;
            } else {
                numad_log(LOG_CRIT, "Could not get node MemFree\n");
                exit(EXIT_FAILURE);
            }
            while (!isdigit(*p)) { p++; }
            CONVERT_DIGITS_TO_NUM(p, KB);
            node[node_ix].MBs_free = (KB / KILOBYTE);
            if (use_inactive_file_cache) {
                // Add inactive file cache quantity to "free" memory
                p = strstr(p, "Inactive(file):");
                if (p != NULL) {
                    p += 15;
                } else {
                    numad_log(LOG_CRIT, "Could not get node Inactive(file)\n");
                    exit(EXIT_FAILURE);
                }
                while (!isdigit(*p)) { p++; }
                CONVERT_DIGITS_TO_NUM(p, KB);
                node[node_ix].MBs_free += (KB / KILOBYTE);
            }
            sum_of_node_MBs_free += node[node_ix].MBs_free;
            if (min_node_MBs_free > node[node_ix].MBs_free) {
                min_node_MBs_free = node[node_ix].MBs_free;
                min_node_MBs_free_ix = node[node_ix].node_id;
            }
            if (max_node_MBs_free < node[node_ix].MBs_free) {
                max_node_MBs_free = node[node_ix].MBs_free;
            }
        } else {
            numad_log(LOG_CRIT, "Could not get node meminfo\n");
            exit(EXIT_FAILURE);
        }
        // If both buffers have been populated by now, sum CPU idle data
        // for each node in order to calculate available capacity
        int old_cpu_data_buf = 1 - cur_cpu_data_buf;
        if (cpu_data_buf[old_cpu_data_buf].time_stamp > 0) {
            uint64_t idle_ticks = 0;
            int cpu = 0;
            int num_lcpus = NUM_IDS_IN_LIST(node[node_ix].cpu_list_p);
            int num_cpus_to_process = num_lcpus;
            while (num_cpus_to_process) {
                if (ID_IS_IN_LIST(cpu, node[node_ix].cpu_list_p)) {
                    idle_ticks += cpu_data_buf[cur_cpu_data_buf].idle[cpu]
                                - cpu_data_buf[old_cpu_data_buf].idle[cpu];
                    num_cpus_to_process -= 1;
                }
                cpu += 1;
            }
            uint64_t time_diff = cpu_data_buf[cur_cpu_data_buf].time_stamp
                               - cpu_data_buf[old_cpu_data_buf].time_stamp;
            // printf("Node: %d   CPUs: %ld   time diff %ld   Idle ticks %ld\n", node_id, node[node_ix].CPUs_total, time_diff, idle_ticks);
            // assert(time_diff > 0);
            node[node_ix].CPUs_free = (idle_ticks * ONE_HUNDRED) / time_diff;
            // Possibly discount hyper-threads
            if ((threads_per_core > 1) && (htt_percent < 100)) {
                uint64_t htt_discount = (num_lcpus - (num_lcpus / threads_per_core)) * (100 - htt_percent);
                if (node[node_ix].CPUs_free > htt_discount) {
                    node[node_ix].CPUs_free -= htt_discount;
                } else {
                    node[node_ix].CPUs_free = 0;
                }
            }
            if (node[node_ix].CPUs_free > node[node_ix].CPUs_total) {
                node[node_ix].CPUs_free = node[node_ix].CPUs_total;
            }
            sum_of_node_CPUs_free += node[node_ix].CPUs_free;
            if (min_node_CPUs_free > node[node_ix].CPUs_free) {
                min_node_CPUs_free = node[node_ix].CPUs_free;
                min_node_CPUs_free_ix = node[node_ix].node_id;
            }
            if (max_node_CPUs_free < node[node_ix].CPUs_free) {
                max_node_CPUs_free = node[node_ix].CPUs_free;
            }
            node[node_ix].magnitude = node[node_ix].CPUs_free * node[node_ix].MBs_free;
        } else {
            node[node_ix].CPUs_free = 0;
            node[node_ix].magnitude = 0;
        }
    }
    avg_node_MBs_free = sum_of_node_MBs_free / num_nodes;
    avg_node_CPUs_free = sum_of_node_CPUs_free / num_nodes;
    double MBs_variance_sum = 0.0;
    double CPUs_variance_sum = 0.0;
    for (int node_ix = 0;  (node_ix < num_nodes);  node_ix++) {
        double MBs_diff = (double)node[node_ix].MBs_free - (double)avg_node_MBs_free;
        double CPUs_diff = (double)node[node_ix].CPUs_free - (double)avg_node_CPUs_free;
        MBs_variance_sum += MBs_diff * MBs_diff;
        CPUs_variance_sum += CPUs_diff * CPUs_diff;
    }
    double MBs_variance = MBs_variance_sum / (num_nodes);
    double CPUs_variance = CPUs_variance_sum / (num_nodes);
    stddev_node_MBs_free = sqrt(MBs_variance);
    stddev_node_CPUs_free = sqrt(CPUs_variance);
    if (log_level >= LOG_INFO) {
        show_nodes();
    }
    return num_nodes;
}


typedef struct stat_data {
    // This structure isn't actually used in numad -- it is here just to
    // document the field type and order of the /proc/<PID>/stat items, some of
    // which are used in the process_data_t structure.
    int pid;              // 0
    char *comm;           // 1
    char state;
    int ppid;
    int pgrp;
    int session;
    int tty_nr;
    int tpgid;
    unsigned flags;
    uint64_t minflt;
    uint64_t cminflt;
    uint64_t majflt;
    uint64_t cmajflt;
    uint64_t utime;       // 13
    uint64_t stime;       // 14
    int64_t cutime;
    int64_t cstime;
    int64_t priority;     // 17
    int64_t nice;
    int64_t num_threads;  // 19
    int64_t itrealvalue;
    uint64_t starttime;
    uint64_t vsize;       // 22
    int64_t rss;          // 23
    uint64_t rsslim;
    uint64_t startcode;
    uint64_t endcode;
    uint64_t startstack;
    uint64_t kstkesp;
    uint64_t kstkeip;
    uint64_t signal;
    uint64_t blocked;
    uint64_t sigignore;
    uint64_t sigcatch;
    uint64_t wchan;
    uint64_t nswap;
    uint64_t cnswap;
    int exit_signal;
    int processor;
    unsigned rt_priority;
    unsigned policy;      // 40
    uint64_t delayacct_blkio_ticks;
    uint64_t guest_time;  // 42
    int64_t cguest_time;
} stat_data_t, *stat_data_p;


process_data_p get_stat_data_for_pid(int pid, char *pid_string) {
    // Note: This function uses static data buffers and is not thread safe.
    char fname[FNAME_SIZE];
    if (pid >= 0) {
        snprintf(fname, FNAME_SIZE, "/proc/%d/stat", pid);
    } else {
        snprintf(fname, FNAME_SIZE, "/proc/%s/stat", pid_string);
    }
    int fd = open(fname, O_RDONLY, 0);
    if (fd < 0) {
        numad_log(LOG_WARNING, "Could not open stat file: %s\n", fname);
        return NULL;
    }
    static char buf[BUF_SIZE];
    int bytes = read(fd, buf, BUF_SIZE);
    close(fd);
    if (bytes < 50) {
        numad_log(LOG_WARNING, "Could not read stat file: %s\n", fname);
        return NULL;
    }
    uint64_t val;
    char *p = buf;
    static process_data_t data;
    // Get PID from field 0
    CONVERT_DIGITS_TO_NUM(p, val);
    data.pid = val;
    // Copy comm from field 1
    while (*p == ' ') { p++; }
    data.comm = p; while (*p != ' ') { p++; }
    *p++ = '\0';   // replacing the presumed single ' ' before next field
    // Skip fields 2 through 12
    for (int ix = 0;  (ix < 11);  ix++) { while (*p != ' ') { p++; } while (*p == ' ') { p++; } }
    // Get utime from field 13 for cpu_util
    CONVERT_DIGITS_TO_NUM(p, val);
    data.cpu_util = val;
    // Get stime from field 14 to add on to cpu_util (which already has utime)
    while (*p == ' ') { p++; }
    CONVERT_DIGITS_TO_NUM(p, val);
    data.cpu_util += val;
    // Skip fields 15 through 18
    while (*p == ' ') { p++; }
    for (int ix = 0;  (ix < 4);  ix++) { while (*p != ' ') { p++; } while (*p == ' ') { p++; } }
    // Get num_threads from field 19
    CONVERT_DIGITS_TO_NUM(p, val);
    data.num_threads = val;
    // Skip fields 20 through 21
    while (*p == ' ') { p++; }
    for (int ix = 0;  (ix < 2);  ix++) { while (*p != ' ') { p++; } while (*p == ' ') { p++; } }
    // Get vsize from field 22 to compute MBs_size
    CONVERT_DIGITS_TO_NUM(p, val);
    data.MBs_size = val / MEGABYTE;
    // Get rss from field 23 to compute MBs_used
    while (*p == ' ') { p++; }
    CONVERT_DIGITS_TO_NUM(p, val);
    data.MBs_used = (val * page_size_in_bytes) / MEGABYTE;
    // Return pointer to data
    return &data;
}


int update_processes() {
    // Conditionally scan /proc/<PID>/stat files for processes we should
    // perhaps manage. For all processes, evaluate whether or not they should
    // be added to our hash table of managed processes candidates.  If so,
    // update the statistics, time stamp and utilization numbers for the select
    // processes in the hash table.
    uint64_t this_update_time = get_time_stamp();
    int new_candidates = 0;  // limit number of new candidates per update
    int files = 0;
    if (scan_all_processes) {
        struct dirent **namelist;
        files = scandir("/proc", &namelist, name_starts_with_digit, NULL);
        if (files < 0) {
            numad_log(LOG_CRIT, "Could not open /proc\n");
            exit(EXIT_FAILURE);
        }
        for (int ix = 0;  (ix < files);  ix++) {
            process_data_p data_p;
            if ((data_p = get_stat_data_for_pid(-1, namelist[ix]->d_name)) != NULL) {
                // See if this process uses enough memory to be managed.
                if ((data_p->MBs_used > MEMORY_THRESHOLD)
                  && (new_candidates < process_hash_table_size / 3)) {
                    data_p->data_time_stamp = get_time_stamp();
                    new_candidates += process_hash_update(data_p);
                }
            }
            free(namelist[ix]);
        }
        free(namelist);
    }  // scan_all_processes
    // Process explicit inclusion and exclusion pid lists
    pthread_mutex_lock(&pid_list_mutex);
    // Include candidate processes from the explicit include pid list
    pid_list_p pid_ptr = include_pid_list;
    while ((pid_ptr != NULL) && (new_candidates < process_hash_table_size / 3)) {
        int hash_ix = process_hash_lookup(pid_ptr->pid);
        if ( (hash_ix >= 0) && (process_hash_table[hash_ix].data_time_stamp > this_update_time)) {
            // Already in hash table, and recently updated...
            pid_ptr = pid_ptr->next;
            continue;
        }
        process_data_p data_p;
        if ((data_p = get_stat_data_for_pid(pid_ptr->pid, NULL)) != NULL) {
            data_p->data_time_stamp = get_time_stamp();
            new_candidates += process_hash_update(data_p);
            if (!scan_all_processes) {
                files += 1;
            }
            pid_ptr = pid_ptr->next;
        } else {
            // no stat file so assume pid dead -- remove it from pid list
            include_pid_list = remove_pid_from_pid_list(include_pid_list, pid_ptr->pid);
            pid_ptr = include_pid_list;  // just restart from list beginning
            continue;
        }
    }
    // Zero CPU utilization for processes on the explicit exclude pid list
    pid_ptr = exclude_pid_list;
    while (pid_ptr != NULL) {
        int hash_ix = process_hash_lookup(pid_ptr->pid);
        if (hash_ix >= 0) {
            process_hash_table[hash_ix].CPUs_used = 0;
        }
        pid_ptr = pid_ptr->next;
    }
    pthread_mutex_unlock(&pid_list_mutex);
    if (log_level >= LOG_INFO) {
        numad_log(LOG_INFO, "Processes: %d\n", files);
    }
    // Now go through all managed processes to cleanup out-of-date and dead ones.
    process_hash_table_cleanup(this_update_time);
    return files;
}


int initialize_mem_node_list(process_data_p p) {
    // Parameter p is a pointer to an element in the hash table
    if ((!p) || (p->pid < 1)) {
        numad_log(LOG_CRIT, "Cannot initialize mem node lists with bad PID\n");
        exit(EXIT_FAILURE);
    }
    int n = 0;
    char fname[FNAME_SIZE];
    char buf[BIG_BUF_SIZE];
    p->process_MBs = NULL;
    CLEAR_NODE_LIST(p->node_list_p);
    snprintf(fname, FNAME_SIZE, "/proc/%d/status", p->pid);
    int fd = open(fname, O_RDONLY, 0);
    if (fd < 0) {
        numad_log(LOG_WARNING, "Tried to research PID %d, but it apparently went away.\n", p->pid);
        return 0;  // Assume the process terminated
    }
    int bytes = read(fd, buf, BIG_BUF_SIZE);
    close(fd);
    if (bytes <= 0) {
        numad_log(LOG_WARNING, "Tried to research PID %d, but cannot read status file.\n", p->pid);
        return 0;  // Assume the process terminated
    } else if (bytes >= BIG_BUF_SIZE) {
        buf[BIG_BUF_SIZE - 1] = '\0';
    } else {
        buf[bytes] = '\0';
    }
    char *list_str_p = strstr(buf, "Mems_allowed_list:");
    if (!list_str_p) {
        numad_log(LOG_CRIT, "Could not get node Mems_allowed_list\n");
        exit(EXIT_FAILURE);
    }
    list_str_p += 18;
    while (!isdigit(*list_str_p)) { list_str_p++; }
    n = add_ids_to_list_from_str(p->node_list_p, list_str_p);
    if (n < num_nodes) {
        // If process already bound to a subset of nodes when we discover it,
        // set initial bind_time_stamp to 30 minutes ago...
        p->bind_time_stamp = get_time_stamp() - (1800 * ONE_HUNDRED);
    }
    return n;
}


uint64_t combined_value_of_weighted_resources(int ix, int mbs, int cpus, uint64_t MBs_free, uint64_t CPUs_free) {
    int64_t needed_mem;
    int64_t needed_cpu;
    int64_t excess_mem;
    int64_t excess_cpu;
    if (MBs_free > mbs) {
        needed_mem = mbs;
        excess_mem = MBs_free - mbs;
    } else {
        needed_mem = MBs_free;
        excess_mem = 0;
    }
    if (CPUs_free > cpus) {
        needed_cpu = cpus;
        excess_cpu = CPUs_free - cpus;
    } else {
        needed_cpu = CPUs_free;
        excess_cpu = 0;
    }
    // Weight the available resources, and then calculate magnitude as
    // product of available CPUs and available MBs.
    int64_t memfactor = (needed_mem * 10 + excess_mem * 4);
    int64_t cpufactor = (needed_cpu *  6 + excess_cpu * 1);
    numad_log(LOG_DEBUG, "    Node[%d]: mem: %ld  cpu: %ld\n", ix, memfactor, cpufactor);
    return (memfactor * cpufactor);
}


id_list_p pick_numa_nodes(int pid, int cpus, int mbs, int assume_enough_cpus) {
    if (log_level >= LOG_DEBUG) {
        numad_log(LOG_DEBUG, "PICK NODES FOR:  PID: %d,  CPUs %d,  MBs %d\n", pid, cpus, mbs);
    }
    char buf[BUF_SIZE];
    uint64_t proc_avg_node_CPUs_free = 0;
    // For existing processes, get miscellaneous process specific details
    int pid_ix;
    process_data_p p = NULL;
    if ((pid > 0) && ((pid_ix = process_hash_lookup(pid)) >= 0)) {
        p = &process_hash_table[pid_ix];
        // Add up per-node memory in use by this process.
        // This scanning is expensive and should be minimized.
        char fname[FNAME_SIZE];
        snprintf(fname, FNAME_SIZE, "/proc/%d/numa_maps", pid);
        FILE *fs = fopen(fname, "r");
        if (!fs) {
            numad_log(LOG_WARNING, "Tried to research PID %d numamaps, but it apparently went away.\n", p->pid);
            return NULL;  // Assume the process terminated
        }
        // Allocate and zero per node memory array.
        // The "+1 node" is for accumulating interleaved memory
        p->process_MBs = realloc(p->process_MBs, (num_nodes + 1) * sizeof(uint64_t));
        if (p->process_MBs == NULL) {
            numad_log(LOG_CRIT, "p->process_MBs realloc failed\n");
            exit(EXIT_FAILURE);
        }
        memset(p->process_MBs, 0, (num_nodes + 1) * sizeof(uint64_t));
        int process_has_interleaved_memory = 0;
        while (fgets(buf, BUF_SIZE, fs)) {
            int interleaved_memory = 0;
            uint64_t page_size = page_size_in_bytes;
            const char *delimiters = " \n";
            char *str_p = strtok(buf, delimiters);
            while (str_p) {
                if (!strncmp(str_p, "interleave", 10)) {
                    interleaved_memory = 1;
                    process_has_interleaved_memory = 1;
                } else if (!strcmp(str_p, "huge")) {
                    page_size = huge_page_size_in_bytes;
                } else if (*str_p++ == 'N') {
                    int node;
                    uint64_t pages;
                    CONVERT_DIGITS_TO_NUM(str_p, node);
                    if (*str_p++ != '=') {
                        numad_log(LOG_CRIT, "numa_maps node number parse error\n");
                        exit(EXIT_FAILURE);
                    }
                    CONVERT_DIGITS_TO_NUM(str_p, pages);
                    p->process_MBs[node] += (pages * page_size);
                    if (interleaved_memory) {
                        // sum interleaved quantity in "extra node"
                        p->process_MBs[num_nodes] += (pages * page_size);
                    }
                }
                // Get next token on the line
                str_p = strtok(NULL, delimiters);
            }
        }
        fclose(fs);
        proc_avg_node_CPUs_free = p->CPUs_used;
        for (int ix = 0;  (ix <= num_nodes);  ix++) {
            p->process_MBs[ix] /= MEGABYTE;
            if ((log_level >= LOG_DEBUG) && (p->process_MBs[ix] > 0)) {
                if (ix == num_nodes) {
                    numad_log(LOG_DEBUG, "Interleaved MBs: %ld\n", ix, p->process_MBs[ix]);
                } else {
                    numad_log(LOG_DEBUG, "PROCESS_MBs[%d]: %ld\n", ix, p->process_MBs[ix]);
                }
            }
            if (ID_IS_IN_LIST(ix, p->node_list_p)) {
                proc_avg_node_CPUs_free += node[ix].CPUs_free;
            }
        }
        proc_avg_node_CPUs_free /= NUM_IDS_IN_LIST(p->node_list_p);
        if ((process_has_interleaved_memory) && (keep_interleaved_memory)) {
            // Mark this process as having interleaved memory so we do not
            // merge the interleaved memory.  Time stamp it as done and return.
            p->flags |= PROCESS_FLAG_INTERLEAVED;
            p->bind_time_stamp = get_time_stamp();
            if (log_level >= LOG_DEBUG) {
                numad_log(LOG_DEBUG, "Skipping evaluation of PID %d because of interleaved memory.\n", p->pid);
            }
            return NULL;
        }
    }  // end of existing PID conditional
    // Make a copy of node available resources array.  Add in info specific to
    // this process to equalize available resource quantities wrt locations of
    // resources already in use by this process.
    static node_data_p tmp_node;
    tmp_node = realloc(tmp_node, num_nodes * sizeof(node_data_t) );
    if (tmp_node == NULL) {
        numad_log(LOG_CRIT, "tmp_node realloc failed\n");
        exit(EXIT_FAILURE);
    }
    memcpy(tmp_node, node, num_nodes * sizeof(node_data_t) );
    uint64_t sum_of_node_CPUs_free = 0;
    for (int ix = 0;  (ix < num_nodes);  ix++) {
        if (pid > 0) {
            if (NUM_IDS_IN_LIST(p->node_list_p) >= num_nodes) {
                // Process not yet bound to a subset of nodes.
                // Add back memory used by this process on this node.
                tmp_node[ix].MBs_free += ((p->process_MBs[ix] * 17) / 16);  // Apply light mem bias
                // Add back CPU used by this process in proportion to the memory used on this node.
                tmp_node[ix].CPUs_free += ((p->CPUs_used * p->process_MBs[ix]) / p->MBs_used);
            } else {
                // If the process is currently running on less than all the
                // nodes, first add back (biased) memory already used by this
                // process on this node, then assign average process CPU / node
                // for this process iff the process is present on this node.
                tmp_node[ix].MBs_free += ((p->process_MBs[ix] * 5) / 4);  // Apply heavy mem bias
                if (ID_IS_IN_LIST(ix, p->node_list_p)) {
                    tmp_node[ix].CPUs_free = proc_avg_node_CPUs_free;
                }
            }
            sum_of_node_CPUs_free += tmp_node[ix].CPUs_free;
            if (tmp_node[ix].CPUs_free > tmp_node[ix].CPUs_total) {
                tmp_node[ix].CPUs_free = tmp_node[ix].CPUs_total;
            }
            if (tmp_node[ix].MBs_free > tmp_node[ix].MBs_total) {
                tmp_node[ix].MBs_free = tmp_node[ix].MBs_total;
            }
        }
        // Enforce 1/100th CPU minimum
        if (tmp_node[ix].CPUs_free < 1) {
            tmp_node[ix].CPUs_free = 1;
        }
        // numad_log(LOG_DEBUG, "Raw Node[%d]: mem: %ld  cpu: %ld\n", ix, tmp_node[ix].MBs_free, tmp_node[ix].CPUs_free);
        tmp_node[ix].magnitude = combined_value_of_weighted_resources(ix, mbs, cpus, tmp_node[ix].MBs_free, tmp_node[ix].CPUs_free);
    }
    // Now figure out where to get resources for this request....
    static id_list_p target_node_list_p;
    CLEAR_NODE_LIST(target_node_list_p);
    if ((pid > 0) && (cpus > sum_of_node_CPUs_free)) {
        // System CPUs might be oversubscribed, but...
        assume_enough_cpus = 1;
        // and rely on available memory for placement.
    }
    // Establish a CPU flex fudge factor, on the presumption it is OK if not
    // quite all the CPU request is met.  However, if trying to find resources
    // for pre-placement advice request, do not underestimate the amount of
    // CPUs needed.  Instead, err on the side of providing too many resources.
    int cpu_flex = 0;
    if ((pid > 0) && (target_utilization < 100)) {
        // FIXME: Is half of the utilization margin a good amount of CPU flexing?
        cpu_flex = ((100 - target_utilization) * node[0].CPUs_total) / 200;
    }
    // Figure out minimum number of nodes required
    int mem_req_nodes = ceil((double)mbs  / (double)node[0].MBs_total);
    int cpu_req_nodes = ceil((double)(cpus - cpu_flex) / (double)node[0].CPUs_total); 
    int min_req_nodes = mem_req_nodes;
    if (min_req_nodes < cpu_req_nodes) {
        min_req_nodes = cpu_req_nodes;
    }
    if (min_req_nodes > num_nodes) {
        min_req_nodes = num_nodes;
    }
    // Use an index to sort NUMA connected resource chain for each node
    int index[num_nodes];
    uint64_t totmag[num_nodes];
    for (int ix = 0;  (ix < num_nodes);  ix++) {
        // Reset the index each time
        for (int n = 0;  (n < num_nodes);  n++) {
            index[n] = n;
        }
        // Sort by minimum relative NUMA distance from node[ix],
        // breaking distance ties with magnitude of available resources
        for (int ij = 0;  (ij < num_nodes);  ij++) {
            int best_ix = ij;
            for (int ik = ij + 1;  (ik < num_nodes);  ik++) {
                int ik_dist = tmp_node[index[ik]].distance[ix];
                int best_ix_dist = tmp_node[index[best_ix]].distance[ix];
                if (best_ix_dist > ik_dist) {
                    best_ix = ik;
                } else if (best_ix_dist == ik_dist) {
                    if (tmp_node[index[best_ix]].magnitude < tmp_node[index[ik]].magnitude ) {
                        best_ix = ik;
                    }
                }
            }
            if (best_ix != ij) {
                int tmp = index[ij];
                index[ij] = index[best_ix];
                index[best_ix] = tmp;
            }
        }
#if 0
        if (log_level >= LOG_DEBUG) {
            for (int iq = 0;  (iq < num_nodes);  iq++) {
                numad_log(LOG_DEBUG, "Node: %d  Dist: %d  Magnitude: %ld\n",
                    tmp_node[index[iq]].node_id, tmp_node[index[iq]].distance[ix], tmp_node[index[iq]].magnitude);
            }
        }
#endif
        // Save the totmag[] sum of the magnitudes of expected needed nodes,
        // "normalized" by NUMA distance (by dividing each magnitude by the
        // relative distance squared).
        totmag[ix] = 0;
        for (int ij = 0;  (ij < min_req_nodes);  ij++) {
            int dist = tmp_node[index[ij]].distance[ix];
            totmag[ix] += (tmp_node[index[ij]].magnitude / (dist * dist));
        }
        numad_log(LOG_DEBUG, "Totmag[%d]: %ld\n", ix, totmag[ix]);
    }
    // Now find the best NUMA node based on the normalized sum of node
    // magnitudes expected to be used.
    int best_node_ix = 0;
    for (int ix = 0;  (ix < num_nodes);  ix++) {
        if (totmag[best_node_ix] < totmag[ix]) {
            best_node_ix = ix;
        }
    }
    numad_log(LOG_DEBUG, "best_node_ix: %d\n", best_node_ix);
    // Reset sorting index again
    for (int n = 0;  (n < num_nodes);  n++) {
        index[n] = n;
    }
    // Sort index by distance from node[best_node_ix],
    // breaking distance ties with magnitude
    for (int ij = 0;  (ij < num_nodes);  ij++) {
        int best_ix = ij;
        for (int ik = ij + 1;  (ik < num_nodes);  ik++) {
            int ik_dist = tmp_node[index[ik]].distance[best_node_ix];
            int best_ix_dist = tmp_node[index[best_ix]].distance[best_node_ix];
            if (best_ix_dist > ik_dist) {
                best_ix = ik;
            } else if (best_ix_dist == ik_dist) {
                if (tmp_node[index[best_ix]].magnitude < tmp_node[index[ik]].magnitude ) {
                    best_ix = ik;
                }
            }
        }
        if (best_ix != ij) {
            int tmp = index[ij];
            index[ij] = index[best_ix];
            index[best_ix] = tmp;
        }
    }
    if (log_level >= LOG_DEBUG) {
        for (int iq = 0;  (iq < num_nodes);  iq++) {
            numad_log(LOG_DEBUG, "Node: %d  Dist: %d  Magnitude: %ld\n",
                tmp_node[index[iq]].node_id, tmp_node[index[iq]].distance[best_node_ix], tmp_node[index[iq]].magnitude);
        }
    }
    // Allocate more resources until request is met.
    best_node_ix = 0;
    while ((min_req_nodes > 0) || (mbs > 0) || ((cpus > cpu_flex) && (!assume_enough_cpus))) {
        if (log_level >= LOG_DEBUG) {
            numad_log(LOG_DEBUG, "MBs: %d,  CPUs: %d\n", mbs, cpus);
        }
        numad_log(LOG_DEBUG, "Assigning resources from node %d\n", index[best_node_ix]);
        ADD_ID_TO_LIST(tmp_node[index[best_node_ix]].node_id, target_node_list_p);
        min_req_nodes -= 1;
        if (EQUAL_LISTS(target_node_list_p, all_nodes_list_p)) {
            // Apparently we must use all resource nodes...
            break;
        }
        // "Consume" the resources on this node
#define CPUS_MARGIN 0
#define MBS_MARGIN 100
        if (tmp_node[index[best_node_ix]].MBs_free >= (mbs + MBS_MARGIN)) {
            tmp_node[index[best_node_ix]].MBs_free -= mbs;
            mbs = 0;
        } else {
            mbs -= (tmp_node[index[best_node_ix]].MBs_free - MBS_MARGIN);
            tmp_node[index[best_node_ix]].MBs_free = MBS_MARGIN;
        }
        if (tmp_node[index[best_node_ix]].CPUs_free >= (cpus + CPUS_MARGIN)) {
            tmp_node[index[best_node_ix]].CPUs_free -= cpus;
            cpus = 0;
        } else {
            cpus -= (tmp_node[index[best_node_ix]].CPUs_free - CPUS_MARGIN);
            tmp_node[index[best_node_ix]].CPUs_free = CPUS_MARGIN;
        }
        // Next line optional, since we will not look at that node again
        tmp_node[index[best_node_ix]].magnitude = combined_value_of_weighted_resources(0, mbs, cpus, tmp_node[index[best_node_ix]].MBs_free, tmp_node[index[best_node_ix]].CPUs_free);
        best_node_ix += 1;
    }
    // For existing processes, calculate the non-local memory percent to see if
    // process is already in the right place.
    if ((pid > 0) && (p != NULL)) {
        uint64_t nonlocal_memory = 0;
        for (int ix = 0;  (ix < num_nodes);  ix++) {
            if (!ID_IS_IN_LIST(ix, target_node_list_p)) {
                // Accumulate total of nonlocal memory
                nonlocal_memory += p->process_MBs[ix];
            }
        }
        int disp_percent = (100 * nonlocal_memory) / p->MBs_used;
        // If this existing process is already located where we want it, then just
        // return NULL indicating no need to change binding this time.  Check the
        // ammount of nonlocal memory against the target_memlocality_perecent.
        if ((disp_percent <= (100 - target_memlocality)) && (p->bind_time_stamp) && (EQUAL_LISTS(target_node_list_p, p->node_list_p))) {
            // Already bound to targets, and enough of the memory is located where we want it, so no need to rebind
            if (log_level >= LOG_DEBUG) {
                numad_log(LOG_DEBUG, "Process %d already %d percent localized to target nodes.\n", p->pid, 100 - disp_percent);
            }
            p->bind_time_stamp = get_time_stamp();
            return NULL;
        }
    }
    // Must always provide at least one node for pre-placement advice
    // FIXME: verify this can happen only if no resources requested...
    if ((pid <= 0) && (NUM_IDS_IN_LIST(target_node_list_p) <= 0)) {
        ADD_ID_TO_LIST(node[0].node_id, target_node_list_p);
    }
    // Log advice, and return target node list
    if ((pid > 0) && (p->bind_time_stamp)) {
        str_from_id_list(buf,  BUF_SIZE, p->node_list_p);
    } else {
        str_from_id_list(buf,  BUF_SIZE, all_nodes_list_p);
    }
    char buf2[BUF_SIZE];
    str_from_id_list(buf2, BUF_SIZE, target_node_list_p);
    char *cmd_name = "(unknown)";
    if ((p) && (p->comm)) {
        cmd_name = p->comm;
    }
    numad_log(LOG_NOTICE, "Advising pid %d %s move from nodes (%s) to nodes (%s)\n", pid, cmd_name, buf, buf2);
    if (pid > 0) {
        COPY_LIST(target_node_list_p, p->node_list_p);
    }
    return target_node_list_p;
}


int manage_loads() {
    uint64_t time_stamp = get_time_stamp();
    // Use temporary index to access and sort hash table entries
    static int pindex_size;
    static process_data_p *pindex;
    if (pindex_size < process_hash_table_size) {
        pindex_size = process_hash_table_size;
        pindex = realloc(pindex, pindex_size * sizeof(process_data_p));
        if (pindex == NULL) {
            numad_log(LOG_CRIT, "pindex realloc failed\n");
            exit(EXIT_FAILURE);
        }
        // Quick round trip whenever we resize the hash table.
        // This is mostly to avoid max_interval wait at start up.
        return min_interval / 2;
    }
    memset(pindex, 0, pindex_size * sizeof(process_data_p));
    // Copy live candidate pointers to the index for sorting
    // if they meet the threshold for memory usage and CPU usage.
    int nprocs = 0;
    long sum_CPUs_used = 0;
    for (int ix = 0;  (ix < process_hash_table_size);  ix++) {
        process_data_p p = &process_hash_table[ix];
        if ((p->pid) && (p->CPUs_used > CPU_THRESHOLD) && (p->MBs_used > MEMORY_THRESHOLD)) {
            pindex[nprocs++] = p;
            sum_CPUs_used += p->CPUs_used;
            // Initialize node list, if not already done for this process.
            if (p->node_list_p == NULL) {
                initialize_mem_node_list(p);
            }
        }
    }
    // Order candidate considerations using timestamps and magnitude: amount of
    // CPU used * amount of memory used.  Not expecting a long list here.  Use
    // a simplistic sort -- however move all not yet bound to front of list and
    // order by decreasing magnitude.  Previously bound processes follow in
    // bins of increasing magnitude treating values within 20% as aquivalent.
    // Within bins, order by bind_time_stamp so oldest bound will be higher
    // priority to evaluate.  Start by moving all unbound to beginning.
    int num_unbound = 0;
    for (int ij = 0;  (ij < nprocs);  ij++) {
        if (pindex[ij]->bind_time_stamp == 0) {
            process_data_p tmp = pindex[num_unbound];
            pindex[num_unbound++] = pindex[ij];
            pindex[ij] = tmp;
        }
    }
    // Sort all unbound so biggest magnitude comes first
    for (int ij = 0;  (ij < num_unbound);  ij++) {
        int best = ij;
        for (int ik = ij + 1;  (ik < num_unbound);  ik++) {
            uint64_t   ik_mag = (pindex[  ik]->CPUs_used * pindex[  ik]->MBs_used);
            uint64_t best_mag = (pindex[best]->CPUs_used * pindex[best]->MBs_used);
            if (ik_mag <= best_mag) continue;
            best = ik;
        }
        if (best != ij) {
            process_data_p tmp = pindex[ij];
            pindex[ij] = pindex[best];
            pindex[best] = tmp;
        }
    }
    // Sort the remaining candidates into bins of increasting magnitude, and by
    // timestamp within bins.
    for (int ij = num_unbound;  (ij < nprocs);  ij++) {
        int best = ij;
        for (int ik = ij + 1;  (ik < nprocs);  ik++) {
            uint64_t   ik_mag = (pindex[  ik]->CPUs_used * pindex[  ik]->MBs_used);
            uint64_t best_mag = (pindex[best]->CPUs_used * pindex[best]->MBs_used);
            uint64_t  min_mag = ik_mag;
            uint64_t diff_mag = best_mag - ik_mag;
            if (diff_mag < 0) {
                diff_mag = -(diff_mag);
                min_mag = best_mag;
            }
            if ((diff_mag > 0) && (min_mag / diff_mag < 5)) {
                // difference > 20 percent.  Use magnitude ordering
                if (ik_mag <= best_mag) continue;
            } else {
                // difference within 20 percent.  Sort these by bind_time_stamp.
                if (pindex[ik]->bind_time_stamp > pindex[best]->bind_time_stamp) continue;
            }
            best = ik;
        }
        if (best != ij) {
            process_data_p tmp = pindex[ij];
            pindex[ij] = pindex[best];
            pindex[best] = tmp;
        }
    }
    // Show the candidate processes in the log file
    if ((log_level >= LOG_INFO) && (nprocs > 0)) {
        numad_log(LOG_INFO, "Candidates: %d\n", nprocs);
        for (int ix = 0;  (ix < nprocs);  ix++) {
            process_data_p p = pindex[ix];
            char buf[BUF_SIZE];
            str_from_id_list(buf, BUF_SIZE, p->node_list_p);
            fprintf(log_fs, "%ld: PID %d: %s, Threads %2ld, MBs_size %6ld, MBs_used %6ld, CPUs_used %4ld, Magnitude %6ld, Nodes: %s\n", 
                p->data_time_stamp, p->pid, p->comm, p->num_threads, p->MBs_size, p->MBs_used, p->CPUs_used, p->MBs_used * p->CPUs_used, buf);
            }
        fflush(log_fs);
    }
    // Estimate desired size (+ margin capacity) and
    // make resource requests for each candidate process
    for (int ix = 0;  (ix < nprocs);  ix++) {
        process_data_p p = pindex[ix];
        // If this process has interleaved memory, recheck it only every 30 minutes...
#define MIN_DELAY_FOR_INTERLEAVE (1800 * ONE_HUNDRED)
        if (((p->flags & PROCESS_FLAG_INTERLEAVED) > 0)
          && (p->bind_time_stamp + MIN_DELAY_FOR_INTERLEAVE > time_stamp)) {
            if (log_level >= LOG_DEBUG) {
                numad_log(LOG_DEBUG, "Skipping evaluation of PID %d because of interleaved memory.\n", p->pid);
            }
            continue;
        }
        // Expand resources needed estimate using target_utilization factor.
        // Start with the CPUs actually used (capped by number of threads) for
        // CPUs required, and the RSS MBs actually used for the MBs
        // requirement,
        int mem_target_utilization = target_utilization;
        int cpu_target_utilization = target_utilization;
        // Cap memory utilization at 100 percent (but allow CPUs to oversubscribe)
        if (mem_target_utilization > 100) {
            mem_target_utilization = 100;
        }
        // If the process virtual memory size is bigger than one node, and it
        // is already using more than 80 percent of a node, then request MBs
        // based on the virtual size rather than on the current amount in use.
        int mb_request;
        if ((p->MBs_size > node[0].MBs_total) && ((p->MBs_used * 5 / 4) > node[0].MBs_total)) {
            mb_request = (p->MBs_size * 100) / mem_target_utilization;
        } else {
            mb_request = (p->MBs_used * 100) / mem_target_utilization;
        }
        int cpu_request = (p->CPUs_used * 100) / cpu_target_utilization;
        // But do not give a process more CPUs than it has threads!
        int thread_limit = p->num_threads;
        // If process looks like a KVM guest, try to limit thread count to the
        // number of vCPU threads.  FIXME: Will need to do something more
        // intelligent than this with guest IO threads when eventually
        // considering devices and IRQs.
        if ((p->comm) && (p->comm[0] == '(') && (p->comm[1] == 'q') && (strcmp(p->comm, "(qemu-kvm)") == 0)) {
            int kvm_vcpu_threads = get_num_kvm_vcpu_threads(p->pid);
            if (thread_limit > kvm_vcpu_threads) {
                thread_limit = kvm_vcpu_threads;
            }
        }
        thread_limit *= ONE_HUNDRED;
        if (cpu_request > thread_limit) {
            cpu_request = thread_limit;
        }
        // If this process was recently bound, enforce a five-minute minimum
        // delay between repeated attempts to potentially move the process.
#define MIN_DELAY_FOR_REEVALUATION (300 * ONE_HUNDRED)
        if (p->bind_time_stamp + MIN_DELAY_FOR_REEVALUATION > time_stamp) {
            // Skip re-evaluation because we just did it recently, but check
            // first for node utilization balance to see if we should
            // re-evaluate this particular process right now.  If this process
            // is running on one of the busiest nodes, go ahead and re-evaluate
            // it if it looks like it should have a better place with
            // sufficient resources.  FIXME: this is currently implemented for
            // only smallish processes that will fit in a single node.
            if ( ( ID_IS_IN_LIST(min_node_CPUs_free_ix, p->node_list_p) || ID_IS_IN_LIST(min_node_MBs_free_ix, p->node_list_p))
                && (cpu_request < node[0].CPUs_total) && (mb_request < node[0].MBs_total) 
                && (abs(min_node_CPUs_free + p->CPUs_used - avg_node_CPUs_free) 
                    + abs((max_node_CPUs_free - p->CPUs_used) - avg_node_CPUs_free) 
                    < (max_node_CPUs_free - min_node_CPUs_free) - CPU_THRESHOLD)  // CPU slop
                && (abs(min_node_MBs_free + p->MBs_used - avg_node_MBs_free)
                    + abs((max_node_MBs_free - p->MBs_used) - avg_node_MBs_free) 
                    < (max_node_MBs_free - min_node_MBs_free)) ) { 
                if (log_level >= LOG_DEBUG) {
                    numad_log(LOG_DEBUG, "Bypassing delay for %d because it looks like it can do better.\n", p->pid);
                }
            } else {
                if (log_level >= LOG_DEBUG) {
                    numad_log(LOG_DEBUG, "Skipping evaluation of PID %d because done too recently.\n", p->pid);
                }
                continue;
            }
        }
        // OK, now pick NUMA nodes for this process and bind it!
        pthread_mutex_lock(&node_info_mutex);
        int assume_enough_cpus = (sum_CPUs_used <= sum_CPUs_total);
        id_list_p node_list_p = pick_numa_nodes(p->pid, cpu_request, mb_request, assume_enough_cpus);
        if ((node_list_p != NULL) && (bind_process_and_migrate_memory(p))) {
            pthread_mutex_unlock(&node_info_mutex);
            // Return minimum interval when actively moving processes
            return min_interval;
        }
        pthread_mutex_unlock(&node_info_mutex);
    }
    // Return maximum interval when no process movement
    return max_interval;
}


void *set_dynamic_options(void *arg) {
    // int arg_value = *(int *)arg;
    char buf[BUF_SIZE];
    for (;;) {
        // Loop here forever waiting for a msg to do something...
        msg_t msg;
        recv_msg(&msg);
        switch (msg.body.cmd) {
        case 'C':
            use_inactive_file_cache = (msg.body.arg1 != 0);
            if (use_inactive_file_cache) {
                numad_log(LOG_NOTICE, "Counting inactive file cache as available\n");
            } else {
                numad_log(LOG_NOTICE, "Counting inactive file cache as unavailable\n");
            }
            break;
        case 'H':
            thp_scan_sleep_ms = msg.body.arg1;
            set_thp_scan_sleep_ms(thp_scan_sleep_ms);
            break;
        case 'i':
            min_interval = msg.body.arg1;
            max_interval = msg.body.arg2;
            if (max_interval <= 0) {
                shut_down_numad();
            }
            numad_log(LOG_NOTICE, "Changing interval to %d:%d\n", msg.body.arg1, msg.body.arg2);
            break;
        case 'K':
            keep_interleaved_memory = (msg.body.arg1 != 0);
            if (keep_interleaved_memory) {
                numad_log(LOG_NOTICE, "Keeping interleaved memory spread across nodes\n");
            } else {
                numad_log(LOG_NOTICE, "Merging interleaved memory to localized NUMA nodes\n");
            }
            break;
        case 'l':
            numad_log(LOG_NOTICE, "Changing log level to %d\n", msg.body.arg1);
            log_level = msg.body.arg1;
            break;
        case 'm':
            numad_log(LOG_NOTICE, "Changing target memory locality to %d\n", msg.body.arg1);
            target_memlocality = msg.body.arg1;
            break;
        case 'p':
            numad_log(LOG_NOTICE, "Adding PID %d to inclusion PID list\n", msg.body.arg1);
            pthread_mutex_lock(&pid_list_mutex);
            exclude_pid_list = remove_pid_from_pid_list(exclude_pid_list, msg.body.arg1);
            include_pid_list = insert_pid_into_pid_list(include_pid_list, msg.body.arg1);
            pthread_mutex_unlock(&pid_list_mutex);
            break;
        case 'r':
            numad_log(LOG_NOTICE, "Removing PID %d from explicit PID lists\n", msg.body.arg1);
            pthread_mutex_lock(&pid_list_mutex);
            include_pid_list = remove_pid_from_pid_list(include_pid_list, msg.body.arg1);
            exclude_pid_list = remove_pid_from_pid_list(exclude_pid_list, msg.body.arg1);
            pthread_mutex_unlock(&pid_list_mutex);
            break;
        case 'S':
            scan_all_processes = (msg.body.arg1 != 0);
            if (scan_all_processes) {
                numad_log(LOG_NOTICE, "Scanning all processes\n");
            } else {
                numad_log(LOG_NOTICE, "Scanning only explicit PID list processes\n");
            }
            break;
        case 't':
            numad_log(LOG_NOTICE, "Changing logical CPU thread percent to %d\n", msg.body.arg1);
            htt_percent = msg.body.arg1;
            node_info_time_stamp = 0; // to force rescan of nodes/cpus soon
            break;
        case 'u':
            numad_log(LOG_NOTICE, "Changing target utilization to %d\n", msg.body.arg1);
            target_utilization = msg.body.arg1;
            break;
        case 'w':
            numad_log(LOG_NOTICE, "Getting NUMA pre-placement advice for %d CPUs and %d MBs\n",
                                    msg.body.arg1, msg.body.arg2);
            pthread_mutex_lock(&node_info_mutex);
            update_nodes();
            id_list_p node_list_p = pick_numa_nodes(-1, msg.body.arg1, msg.body.arg2, 0);
            str_from_id_list(buf, BUF_SIZE, node_list_p);
            pthread_mutex_unlock(&node_info_mutex);
            send_msg(msg.body.src_pid, 'w', 0, 0, buf);
            break;
        case 'x':
            numad_log(LOG_NOTICE, "Adding PID %d to exclusion PID list\n", msg.body.arg1);
            pthread_mutex_lock(&pid_list_mutex);
            include_pid_list = remove_pid_from_pid_list(include_pid_list, msg.body.arg1);
            exclude_pid_list = insert_pid_into_pid_list(exclude_pid_list, msg.body.arg1);
            pthread_mutex_unlock(&pid_list_mutex);
            break;
        default:
            numad_log(LOG_WARNING, "Unexpected msg command: %c %d %d %s from PID %d\n",
                                    msg.body.cmd, msg.body.arg1, msg.body.arg1, msg.body.text,
                                    msg.body.src_pid);
            break;
        }
    }  // for (;;)
}



void parse_two_arg_values(char *p, int *first_ptr, int *second_ptr, int first_is_optional, int first_scale_digits) {
    char *orig_p = p;
    char *q = NULL;
    int second = -1;
    errno = 0;
    int first = (int) strtol(p, &p, 10);
    if ((errno != 0) || (p == orig_p) || (first < 0)) {
        fprintf(stderr, "Can't parse arg value(s): %s\n", orig_p);
        exit(EXIT_FAILURE);
    }
    if (*p == '.') {
        p++;
        while ((first_scale_digits > 0) && (isdigit(*p))) {
            first *= 10;
            first += (*p++ - '0');
            first_scale_digits -= 1;
        }
        while (isdigit(*p)) { p++; }
    }
    while (first_scale_digits > 0) {
        first *= 10;
        first_scale_digits -= 1;
    }
    if (*p == ':') {
        q = p + 1;
        errno = 0;
        second = (int) strtol(q, &p, 10);
        if ((errno != 0) || (p == q) || (second < 0)) {
            fprintf(stderr, "Can't parse arg value(s): %s\n", orig_p);
            exit(EXIT_FAILURE);
        }
    }
    if (q != NULL) {
        // Two numbers are present
        if (first_ptr  != NULL) *first_ptr = first;
        if (second_ptr != NULL) *second_ptr = second;
    } else if (first_is_optional) {
        if (second_ptr != NULL) *second_ptr = first;
    } else {
        if (first_ptr != NULL) *first_ptr = first;
    }
}



int main(int argc, char *argv[]) {
    int opt;
    int C_flag = 0;
    int d_flag = 0;
    int H_flag = 0;
    int i_flag = 0;
    int K_flag = 0;
    int l_flag = 0;
    int m_flag = 0;
    int p_flag = 0;
    int r_flag = 0;
    int S_flag = 0;
    int t_flag = 0;
    int u_flag = 0;
    int v_flag = 0;
    int w_flag = 0;
    int x_flag = 0;
    int tmp_int = 0;
    long list_pid = 0;
    while ((opt = getopt(argc, argv, "C:dD:hH:i:K:l:p:r:R:S:t:u:vVw:x:")) != -1) {
        switch (opt) {
        case 'C':
            C_flag = 1;
            use_inactive_file_cache = (atoi(optarg) != 0);
            break;
        case 'd':
            d_flag = 1;
            log_level = LOG_DEBUG;
            break;
        case 'D':
            // obsoleted
            break;
        case 'h':
            print_usage_and_exit(argv[0]);
            break;
        case 'H':
            tmp_int = atoi(optarg);
            if ((tmp_int == 0) || ((tmp_int > 9) && (tmp_int < 1000001))) {
                // 0 means do not change the system default value
                H_flag = 1;
                thp_scan_sleep_ms = tmp_int;
            } else {
		fprintf(stderr, "THP scan_sleep_ms must be > 9 and < 1000001\n");
		exit(EXIT_FAILURE);
	    }
            break;
        case 'i':
            i_flag = 1;
            parse_two_arg_values(optarg, &min_interval, &max_interval, 1, 0);
            break;
        case 'K':
            K_flag = 1;
            keep_interleaved_memory = (atoi(optarg) != 0);
            break;
        case 'l':
            l_flag = 1;
            log_level = atoi(optarg);
            break;
        case 'm':
            tmp_int = atoi(optarg);
            if ((tmp_int >= 50) && (tmp_int <= 100)) {
                m_flag = 1;
                target_memlocality = tmp_int;
            }
            break;
        case 'p':
            p_flag = 1;
            list_pid = atol(optarg);
            exclude_pid_list = remove_pid_from_pid_list(exclude_pid_list, list_pid);
            include_pid_list = insert_pid_into_pid_list(include_pid_list, list_pid);
            break;
        case 'r':
            r_flag = 1;
            list_pid = atol(optarg);
            // Remove this PID from both explicit pid lists.
            include_pid_list = remove_pid_from_pid_list(include_pid_list, list_pid);
            exclude_pid_list = remove_pid_from_pid_list(exclude_pid_list, list_pid);
            break;
        case 'R':
            reserved_cpu_str = strdup(optarg);
            break;
        case 'S':
            S_flag = 1;
            scan_all_processes = (atoi(optarg) != 0);
            break;
        case 't':
            tmp_int = atoi(optarg);
            if ((tmp_int >= 0) && (tmp_int <= 100)) {
                t_flag = 1;
                htt_percent = tmp_int;
            }
            break;
        case 'u':
            tmp_int = atoi(optarg);
            if ((tmp_int >= 10) && (tmp_int <= 130)) {
                u_flag = 1;
                target_utilization = tmp_int;
            }
            break;
        case 'v':
            v_flag = 1;
            log_level = LOG_INFO;
            break;
        case 'V':
            print_version_and_exit(argv[0]);
            break;
        case 'w':
            w_flag = 1;
            parse_two_arg_values(optarg, &requested_cpus, &requested_mbs, 0, 2);
            break;
        case 'x':
            x_flag = 1;
            list_pid = atol(optarg);
            include_pid_list = remove_pid_from_pid_list(include_pid_list, list_pid);
            exclude_pid_list = insert_pid_into_pid_list(exclude_pid_list, list_pid);
            break;
        default:
            print_usage_and_exit(argv[0]);
            break;
        }
    }
    if (argc > optind) {
        fprintf(stderr, "Unexpected arg = %s\n", argv[optind]);
        exit(EXIT_FAILURE);
    }
    if (i_flag) {
        if ((max_interval < min_interval) && (max_interval != 0)) {
            fprintf(stderr, "Max interval (%d) must be greater than min interval (%d)\n", max_interval, min_interval);
            exit(EXIT_FAILURE);
        }
    }
    open_log_file();
    init_msg_queue();
    num_cpus = get_num_cpus();
    page_size_in_bytes = sysconf(_SC_PAGESIZE);
    huge_page_size_in_bytes = get_huge_page_size_in_bytes();
    // Figure out if this is the daemon, or a subsequent invocation
    int daemon_pid = get_daemon_pid();
    if (daemon_pid > 0) {
        // Daemon is already running.  So send dynamic options to persistant
        // thread to handle requests, get the response (if any), and finish.
        msg_t msg; 
        if (C_flag) {
            send_msg(daemon_pid, 'C', use_inactive_file_cache, 0, "");
        }
        if (H_flag) {
            send_msg(daemon_pid, 'H', thp_scan_sleep_ms, 0, "");
        }
        if (i_flag) {
            send_msg(daemon_pid, 'i', min_interval, max_interval, "");
        }
        if (K_flag) {
            send_msg(daemon_pid, 'K', keep_interleaved_memory, 0, "");
        }
        if (d_flag || l_flag || v_flag) {
            send_msg(daemon_pid, 'l', log_level, 0, "");
        }
        if (m_flag) {
            send_msg(daemon_pid, 'm', target_memlocality, 0, "");
        }
        if (p_flag) {
            send_msg(daemon_pid, 'p', list_pid, 0, "");
        }
        if (r_flag) {
            send_msg(daemon_pid, 'r', list_pid, 0, "");
        }
        if (S_flag) {
            send_msg(daemon_pid, 'S', scan_all_processes, 0, "");
        }
        if (t_flag) {
            send_msg(daemon_pid, 't', htt_percent, 0, "");
        }
        if (u_flag) {
            send_msg(daemon_pid, 'u', target_utilization, 0, "");
        }
        if (w_flag) {
            send_msg(daemon_pid, 'w', requested_cpus, requested_mbs, "");
            recv_msg(&msg);
            fprintf(stdout, "%s\n", msg.body.text);
        }
        if (x_flag) {
            send_msg(daemon_pid, 'x', list_pid, 0, "");
        }
        close_log_file();
        exit(EXIT_SUCCESS);
    }
    // No numad daemon running yet.
    // First, make note of any reserved CPUs....
    if (reserved_cpu_str != NULL) {
        CLEAR_CPU_LIST(reserved_cpu_mask_list_p);
        int n = add_ids_to_list_from_str(reserved_cpu_mask_list_p, reserved_cpu_str);
        char buf[BUF_SIZE];
        str_from_id_list(buf, BUF_SIZE, reserved_cpu_mask_list_p);
        numad_log(LOG_NOTICE, "Reserving %d CPUs (%s) for non-numad use\n", n, buf);
        // turn reserved list into a negated mask for later ANDing use...
        negate_cpu_list(reserved_cpu_mask_list_p);
    }
    // If it is a "-w" pre-placement request, handle that without starting
    // the daemon.  Otherwise start the numad daemon.
    if (w_flag) {
        // Get pre-placement NUMA advice without starting daemon
        update_nodes();
        sleep(2);
        update_nodes();
        numad_log(LOG_NOTICE, "Getting NUMA pre-placement advice for %d CPUs and %d MBs\n", requested_cpus, requested_mbs);
        id_list_p node_list_p = pick_numa_nodes(-1, requested_cpus, requested_mbs, 0);
        char buf[BUF_SIZE];
        str_from_id_list(buf, BUF_SIZE, node_list_p);
        fprintf(stdout, "%s\n", buf);
        close_log_file();
        exit(EXIT_SUCCESS);
    } else if (max_interval > 0) {
        // Start the numad daemon...
        check_prereqs(argv[0]);
#if (!NO_DAEMON)
        // Daemonize self...
        daemon_pid = fork();
        if (daemon_pid < 0) { numad_log(LOG_CRIT, "fork() failed\n"); exit(EXIT_FAILURE); }
        // Parent process now exits
        if (daemon_pid > 0) { exit(EXIT_SUCCESS); }
        // Child process continues...
        umask(S_IWGRP | S_IWOTH); // Reset the file mode
        int sid = setsid();  // Start a new session
        if (sid < 0) { numad_log(LOG_CRIT, "setsid() failed\n"); exit(EXIT_FAILURE); }
        if ((chdir("/")) < 0) { numad_log(LOG_CRIT, "chdir() failed"); exit(EXIT_FAILURE); }
        daemon_pid = register_numad_pid();
        if (daemon_pid != getpid()) {
            numad_log(LOG_CRIT, "Could not register daemon PID\n");
            exit(EXIT_FAILURE);
        }
        fclose(stdin);
        fclose(stdout);
        if (log_fs != stderr) {
            fclose(stderr);
        }
#endif
        // Set up signal handlers
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa)); 
        sa.sa_handler = sig_handler;
        if (sigaction(SIGHUP, &sa, NULL)
            || sigaction(SIGTERM, &sa, NULL)
            || sigaction(SIGQUIT, &sa, NULL)) {
            numad_log(LOG_CRIT, "sigaction does not work?\n");
            exit(EXIT_FAILURE);
        }
        // Allocate initial process hash table
        process_hash_table_expand();
        // Spawn a thread to handle messages from subsequent invocation requests
        pthread_mutex_init(&pid_list_mutex, NULL);
        pthread_mutex_init(&node_info_mutex, NULL);
        pthread_attr_t attr;
        if (pthread_attr_init(&attr) != 0) {
            numad_log(LOG_CRIT, "pthread_attr_init failure\n");
            exit(EXIT_FAILURE);
        }
        pthread_t tid;
        if (pthread_create(&tid, &attr, &set_dynamic_options, &tid) != 0) {
            numad_log(LOG_CRIT, "pthread_create failure: setting thread\n");
            exit(EXIT_FAILURE);
        }
        // Loop here forwever...
        for (;;) {
            int interval = max_interval;
            pthread_mutex_lock(&node_info_mutex);
            int nodes = update_nodes();
            pthread_mutex_unlock(&node_info_mutex);
            if (nodes > 1) {
                update_processes();
                interval = manage_loads();
                if (interval < max_interval) {
                    // Update node info since we moved something
                    nodes = update_nodes();
                }
            }
            sleep(interval);
            if (got_sigterm | got_sigquit) {
                shut_down_numad();
            }
            if (got_sighup) {
                got_sighup = 0;
                close_log_file();
                open_log_file();
            }
        }
        if (pthread_attr_destroy(&attr) != 0) {
            numad_log(LOG_WARNING, "pthread_attr_destroy failure\n");
        }
        pthread_mutex_destroy(&pid_list_mutex);
        pthread_mutex_destroy(&node_info_mutex);
    }
    exit(EXIT_SUCCESS);
}

