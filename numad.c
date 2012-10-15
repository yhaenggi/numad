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


// Compile with: gcc -O -std=gnu99 -Wall -pthread -o numad numad.c -lrt


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
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/syslog.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <values.h>


#define VERSION_STRING "20121015"


#define VAR_RUN_FILE "/var/run/numad.pid"
#define VAR_LOG_FILE "/var/log/numad.log"

char *cpuset_dir = NULL;
char *cpuset_dir_list[] =  {
    NULL,
    "/sys/fs/cgroup/cpuset",
    "/cgroup/cpuset",
    NULL
};


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
#define TARGET_UTILIZATION_PERCENT 85
#define IMPROVEMENT_THRESHOLD_PERCENT 5


#define ELIM_NEW_LINE(s) \
    if (s[strlen(s) - 1] == '\n') { \
        s[strlen(s) - 1] = '\0'; \
    }

#define CONVERT_DIGITS_TO_NUM(p, n) \
    n = *p++ - '0'; \
    while (isdigit(*p)) { \
        n *= 10; \
        n += (*p++ - '0'); \
    }


int num_cpus = 0;
int num_nodes = 0;
int page_size_in_bytes = 0;
int huge_page_size_in_bytes = 0;

int min_interval = MIN_INTERVAL;
int max_interval = MAX_INTERVAL;
int target_utilization  = TARGET_UTILIZATION_PERCENT;
int scan_all_processes = 1;

pthread_mutex_t pid_list_mutex;
pthread_mutex_t node_info_mutex;
int requested_mbs = 0;
int requested_cpus = 0;



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
    sprintf(buf, ctime(&ts));
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
        numad_log(LOG_ERR, "Cannot open numad log file -- using stderr\n");
    }
}

void close_log_file() {
    if (log_fs != NULL) {
        fclose(log_fs);
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
    // Use CPU_SET(3) <sched.h> cpuset bitmasks,
    // but bundle size and pointer together
    // and genericize for both CPU and Node IDs
    cpu_set_t *set_p; 
    size_t bytes;
} id_list_t, *id_list_p;

#define INIT_ID_LIST(list_p) \
    list_p = malloc(sizeof(id_list_t)); \
    if (list_p == NULL) { numad_log(LOG_CRIT, "INIT_ID_LIST malloc failed\n"); exit(EXIT_FAILURE); } \
    list_p->set_p = CPU_ALLOC(num_cpus); \
    if (list_p->set_p == NULL) { numad_log(LOG_CRIT, "CPU_ALLOC failed\n"); exit(EXIT_FAILURE); } \
    list_p->bytes = CPU_ALLOC_SIZE(num_cpus);

#define CLEAR_LIST(list_p) \
    if (list_p == NULL) { \
        INIT_ID_LIST(list_p); \
    } \
    CPU_ZERO_S(list_p->bytes, list_p->set_p)

#define FREE_LIST(list_p) \
    if (list_p != NULL) { \
        if (list_p->set_p != NULL) { CPU_FREE(list_p->set_p); } \
        free(list_p); \
        list_p = NULL; \
    }

#define NUM_IDS_IN_LIST(list_p)     CPU_COUNT_S(list_p->bytes, list_p->set_p)
#define ADD_ID_TO_LIST(k, list_p)  CPU_SET_S(k, list_p->bytes, list_p->set_p)
#define CLR_ID_IN_LIST(k, list_p)  CPU_CLR_S(k, list_p->bytes, list_p->set_p)
#define ID_IS_IN_LIST(k, list_p) CPU_ISSET_S(k, list_p->bytes, list_p->set_p)

#define           EQUAL_LISTS(list_1_p, list_2_p) CPU_EQUAL_S(list_1_p->bytes,                    list_1_p->set_p, list_2_p->set_p)
#define AND_LISTS(and_list_p, list_1_p, list_2_p) CPU_AND_S(and_list_p->bytes, and_list_p->set_p, list_1_p->set_p, list_2_p->set_p)
#define  OR_LISTS( or_list_p, list_1_p, list_2_p)  CPU_OR_S( or_list_p->bytes,  or_list_p->set_p, list_1_p->set_p, list_2_p->set_p)
#define XOR_LISTS(xor_list_p, list_1_p, list_2_p) CPU_XOR_S(xor_list_p->bytes, xor_list_p->set_p, list_1_p->set_p, list_2_p->set_p)

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

// RING_BUF_SIZE must be a power of two
#define RING_BUF_SIZE 8

#define PROCESS_FLAG_INTERLEAVED (1 << 0)

typedef struct process_data {
    int pid;
    unsigned int flags;
    uint64_t data_time_stamp; // hundredths of seconds
    uint64_t bind_time_stamp;
    uint64_t num_threads;
    uint64_t MBs_used;
    uint64_t cpu_util;
    uint64_t CPUs_used;  // scaled * ONE_HUNDRED
    uint64_t CPUs_used_ring_buf[RING_BUF_SIZE];
    int ring_buf_ix;
    int dup_bind_count;
    char *comm;
    char *cpuset_name;
} process_data_t, *process_data_p;



// Hash table size must always be a power of two
#define MIN_PROCESS_HASH_TABLE_SIZE 64
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
    // This updates hash table stats for processes we are monitoring
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
        p->MBs_used = newp->MBs_used;
        p->cpu_util = newp->cpu_util;
        p->num_threads = newp->num_threads;
        p->data_time_stamp = newp->data_time_stamp;
    }
    return new_hash_table_entry;
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
        if (dp->comm)   { free(dp->comm); }
        if (dp->cpuset_name) { free(dp->cpuset_name); }
        // if (dp->node_list_p) { FREE_LIST(dp->node_list_p); }
        memset(dp, 0, sizeof(process_data_t));
        // bubble up the collision chain
        while ((pid = process_hash_table[++ix].pid) > 0) {
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

void process_hash_table_cleanup(uint64_t update_time) {
    int cpusets_removed = 0;
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
                char fname[FNAME_SIZE];
                snprintf(fname, FNAME_SIZE, "/proc/%d", p->pid);
                if (access(fname, F_OK) < 0) {
                    // Seems dead.  Forget this pid -- after first checking 
                    // and removing obsolete numad.PID cpuset directories.  
                    snprintf(fname, FNAME_SIZE, "%s/numad.%d", cpuset_dir, p->pid);
                    if (access(fname, F_OK) == 0) {
                        numad_log(LOG_NOTICE, "Removing obsolete cpuset: %s\n", fname);
                        int rc = rmdir(fname);
                        if (rc >= 0) {
                            cpusets_removed += 1;
                        } else {
                            numad_log(LOG_ERR, "bad cpuset rmdir\n");
                            // exit(EXIT_FAILURE);
                        }
                    }
                    process_hash_remove(p->pid);
                    num_hash_entries_used -= 1;
                }
            }
        }
    }
    if (cpusets_removed > 0) {
        // Expire all the duplicate bind counts so things will be re-evaluated sooner.
        for (int ix = 0;  (ix < process_hash_table_size);  ix++) {
            process_hash_table[ix].dup_bind_count = 0;
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
            // Clear dup_bind_count, in case user wants it to be re-evaluated soon
            process_hash_table[hash_ix].dup_bind_count = 0;
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
    fprintf(stderr, "-d for debug logging (same effect as '-l 7')\n");
    fprintf(stderr, "-D <CGROUP_MOUNT_POINT> to specify cgroup mount point\n");
    fprintf(stderr, "-h to print this usage info\n");
    fprintf(stderr, "-i [<MIN>:]<MAX> to specify interval seconds\n");
    fprintf(stderr, "-l <N> to specify logging level (usually 5, 6, or 7)\n");
    fprintf(stderr, "-p <PID> to add PID to inclusion pid list\n");
    fprintf(stderr, "-r <PID> to remove PID from explicit pid lists\n");
    fprintf(stderr, "-S 1  to scan all processes\n");
    fprintf(stderr, "-S 0  to scan only explicit PID list processes\n");
    fprintf(stderr, "-u <N> to specify target utilization percent (default 85)\n");
    fprintf(stderr, "-v for verbose  (same effect as '-l 6')\n");
    fprintf(stderr, "-V to show version info\n");
    fprintf(stderr, "-w <CPUs>[:<MBs>] for NUMA node suggestions\n");
    fprintf(stderr, "-x <PID> to add PID to exclusion pid list\n");
    exit(EXIT_FAILURE);
}


void check_prereqs(char *prog_name) {
    // Verify cpusets are available on this system.
    char **dir = &cpuset_dir_list[0];
    if (*dir == NULL) { dir++; }
    while (*dir != NULL) {
        cpuset_dir = *dir;
        char fname[FNAME_SIZE];
        snprintf(fname, FNAME_SIZE, "%s/cpuset.cpus", cpuset_dir);
        if (access(fname, F_OK) == 0) {
            break;
        }
        dir++;
    }
    if (*dir == NULL) {
        fprintf(stderr, "\n");
        fprintf(stderr, "Are CPUSETs enabled on this system?\n");
        fprintf(stderr, "They are required for %s to function.\n\n", prog_name);
        fprintf(stderr, "Check manpage CPUSET(7). You might need to do something like:\n");
        fprintf(stderr, "    # mkdir <DIRECTORY_MOUNT_POINT>\n");
        fprintf(stderr, "    # mount cgroup -t cgroup -o cpuset <DIRECTORY_MOUNT_POINT>\n");
        fprintf(stderr, "    where <DIRECTORY_MOUNT_POINT> is something like:\n");
        dir = &cpuset_dir_list[0];
        if (*dir == NULL) { dir++; }
        while (*dir != NULL) {
            fprintf(stderr, "      - %s\n", *dir);
            dir++;
        }
        fprintf(stderr, "and then try again...\n");
        fprintf(stderr, "Or, use '-D <DIRECTORY_MOUNT_POINT>' to specify the correct mount point\n");
        fprintf(stderr, "\n");
        exit(EXIT_FAILURE);
    }
    // Check on THP scan sleep time.
    char *thp_scan_fname = "/sys/kernel/mm/redhat_transparent_hugepage/khugepaged/scan_sleep_millisecs";
    int fd = open(thp_scan_fname, O_RDONLY, 0);
    if (fd >= 0) {
        int ms;
        char buf[BUF_SIZE];
        int bytes = read(fd, buf, BUF_SIZE);
        close(fd);
        if (bytes > 0) {
            char *p = buf;
            CONVERT_DIGITS_TO_NUM(p, ms);
            if (ms > 150) {
                fprintf(stderr, "\n");
                numad_log(LOG_NOTICE, "Looks like transparent hugepage scan time in %s is %d ms.\n", thp_scan_fname, ms);
                fprintf(stderr,       "Looks like transparent hugepage scan time in %s is %d ms.\n", thp_scan_fname, ms);
                fprintf(stderr, "Consider increasing the frequency of THP scanning,\n");
                fprintf(stderr, "by echoing a smaller number (e.g. 100) to %s\n", thp_scan_fname);
                fprintf(stderr, "to more agressively (re)construct THPs.  For example:\n");
                fprintf(stderr, "# echo 100 > /sys/kernel/mm/redhat_transparent_hugepage/khugepaged/scan_sleep_millisecs\n");
                fprintf(stderr, "\n");
            }
        }
    }
    // FIXME: ?? check for enabled ksmd, and recommend disabling ksm?
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
    // FIXME: figure out some better way to do this...
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


int get_huge_page_size_in_bytes() {
    int huge_page_size = 0;;
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
	    huge_page_size = atoi(p);
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


int bind_process_and_migrate_memory(int pid, char *cpuset_name, id_list_p node_list_p, id_list_p cpu_list_p) {
    // Check basic parameter validity.  
    if (pid <= 0) {
        numad_log(LOG_CRIT, "Bad PID to bind\n");
        exit(EXIT_FAILURE);
    }
    if ((cpuset_name == NULL) || (strlen(cpuset_name) == 0)) {
        numad_log(LOG_CRIT, "Bad cpuset name to bind\n");
        exit(EXIT_FAILURE);
    }
    int nodes;
    if ((node_list_p == NULL) || ((nodes = NUM_IDS_IN_LIST(node_list_p)) == 0)) {
        numad_log(LOG_CRIT, "Cannot bind to unspecified node\n");
        exit(EXIT_FAILURE);
    }
    // Cpu_list_p is optional and may be NULL...
    // Generate CPU id list from the specified node list if necessary
    if (cpu_list_p == NULL) {
        static id_list_p tmp_cpu_list_p;
        CLEAR_LIST(tmp_cpu_list_p);
        int node_id = 0;
        while (nodes) {
            if (ID_IS_IN_LIST(node_id, node_list_p)) {
                OR_LISTS(tmp_cpu_list_p, tmp_cpu_list_p, node[node_id].cpu_list_p);
                nodes -= 1;
            }
            node_id += 1;
        }
        cpu_list_p = tmp_cpu_list_p;
    }
    // Make the cpuset directory if necessary
    char cpuset_name_buf[FNAME_SIZE];
    snprintf(cpuset_name_buf, FNAME_SIZE, "%s%s", cpuset_dir, cpuset_name);
    char *p = &cpuset_name_buf[strlen(cpuset_dir)];
    if (!strcmp(p, "/")) {
        // Make a cpuset directory for this process
        snprintf(cpuset_name_buf, FNAME_SIZE, "%s/numad.%d", cpuset_dir, pid);
        numad_log(LOG_NOTICE, "Making new cpuset: %s\n", cpuset_name_buf);
        int rc = mkdir(cpuset_name_buf, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        if (rc == -1) {
            numad_log(LOG_CRIT, "Bad cpuset mkdir -- errno: %d\n", errno);
            return 0;
        }
    }
    cpuset_name = cpuset_name_buf;
    // Now that we have a cpuset for pid and a populated cpulist,
    // start the actual binding and migration.
    uint64_t t0 = get_time_stamp();

    // Write "1" out to cpuset.memory_migrate file
    char fname[FNAME_SIZE];
    snprintf(fname, FNAME_SIZE, "%s/cpuset.memory_migrate", cpuset_name);
    int fd = open(fname, O_WRONLY | O_TRUNC, 0);
    if (fd == -1) {
        numad_log(LOG_CRIT, "Could not open cpuset.memory_migrate -- errno: %d\n", errno);
        return 0;
    }
    write(fd, "1", 1);
    close(fd);

    // Write node IDs out to cpuset.mems file
    char node_list_buf[BUF_SIZE];
    snprintf(fname, FNAME_SIZE, "%s/cpuset.mems", cpuset_name);
    fd = open(fname, O_WRONLY | O_TRUNC, 0);
    if (fd == -1) {
        numad_log(LOG_CRIT, "Could not open cpuset.mems -- errno: %d\n", errno);
        return 0;
    }
    int len = str_from_id_list(node_list_buf, BUF_SIZE, node_list_p);
    write(fd, node_list_buf, len);
    close(fd);

    // Write CPU IDs out to cpuset.cpus file
    char cpu_list_buf[BUF_SIZE];
    snprintf(fname, FNAME_SIZE, "%s/cpuset.cpus", cpuset_name);
    fd = open(fname, O_WRONLY | O_TRUNC, 0);
    if (fd == -1) {
        numad_log(LOG_CRIT, "Could not open cpuset.cpus -- errno: %d\n", errno);
        return 0;
    }
    len = str_from_id_list(cpu_list_buf, BUF_SIZE, cpu_list_p);
    write(fd, cpu_list_buf, len);
    close(fd);

    // Copy pid tasks one at a time to tasks file
    snprintf(fname, FNAME_SIZE, "%s/tasks", cpuset_name);
    fd = open(fname, O_WRONLY | O_TRUNC, 0);
    if (fd == -1) {
        numad_log(LOG_CRIT, "Could not open tasks -- errno: %d\n", errno);
        return 0;
    }
    snprintf(fname, FNAME_SIZE, "/proc/%d/task", pid);
    struct dirent **namelist;
    int files = scandir(fname, &namelist, name_starts_with_digit, NULL);
    if (files < 0) {
        numad_log(LOG_WARNING, "Could not scandir task list\n");
        return 0;  // Assume the process terminated
    }
    for (int ix = 0;  (ix < files);  ix++) {
        // copy pid tasks, one at a time
        numad_log(LOG_NOTICE, "Including task: %s\n", namelist[ix]->d_name);
        write(fd, namelist[ix]->d_name, strlen(namelist[ix]->d_name));
        free(namelist[ix]);
    }
    free(namelist);
    close(fd);

    uint64_t t1 = get_time_stamp();
    // Check pid still active
    snprintf(fname, FNAME_SIZE, "/proc/%d", pid);
    if (access(fname, F_OK) < 0) {
        numad_log(LOG_WARNING, "Could not migrate pid\n");
        return 0;  // Assume the process terminated
    }
    numad_log(LOG_NOTICE, "PID %d moved to node(s) %s in %d.%d seconds\n", pid, node_list_buf, (t1-t0)/100, (t1-t0)%100);
    return 1;
}


void show_nodes() {
    time_t ts = time(NULL);
    fprintf(log_fs, "%s", ctime(&ts));
    fprintf(log_fs, "Nodes: %d\n", num_nodes);
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
    fprintf(log_fs, "\n");
    fflush(log_fs);
}


typedef struct cpu_data {
    uint64_t time_stamp;
    uint64_t *idle;
} cpu_data_t, *cpu_data_p;

cpu_data_t cpu_data_buf[2];  // Two sets, to calc deltas
int cur_cpu_data_buf = 0;


void update_cpu_data() {
    // Parse idle percents from CPU stats in /proc/stat cpu<N> lines
    static FILE *fs = NULL;
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
            uint64_t idle = *p++ - '0'; while (isdigit(*p)) { idle *= 10; idle += (*p++ - '0'); }
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


id_list_p all_cpus_list_p = NULL;
id_list_p all_nodes_list_p = NULL;
uint64_t node_info_time_stamp = 0;


int update_nodes() {
    char fname[FNAME_SIZE];
    char buf[BIG_BUF_SIZE];
    // First, check to see if we should refresh basic node info that probably never changes...
    uint64_t time_stamp = get_time_stamp();
#define STATIC_NODE_INFO_DELAY (600 * ONE_HUNDRED)
    if ((num_nodes == 0) || (node_info_time_stamp + STATIC_NODE_INFO_DELAY < time_stamp)) {
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
        CLEAR_LIST(all_cpus_list_p);
        CLEAR_LIST(all_nodes_list_p);
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
                // get cpulist from the cpulist string
                CLEAR_LIST(node[node_ix].cpu_list_p);
                int n = add_ids_to_list_from_str(node[node_ix].cpu_list_p, buf);
                OR_LISTS(all_cpus_list_p, all_cpus_list_p, node[node_ix].cpu_list_p);
                node[node_ix].CPUs_total = n * ONE_HUNDRED;
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
    // Second, get the dynamic free memory and available CPU capacity
    update_cpu_data();
    for (int node_ix = 0;  (node_ix < num_nodes);  node_ix++) {
        int node_id = node[node_ix].node_id;
        // Get available memory info from node<N>/meminfo file
        snprintf(fname, FNAME_SIZE, "/sys/devices/system/node/node%d/meminfo", node_id);
        int fd = open(fname, O_RDONLY, 0);
        if ((fd >= 0) && (read(fd, buf, BIG_BUF_SIZE) > 0)) {
            uint64_t KB;
            char *p = strstr(buf, "MemTotal:");
            if (p != NULL) {
                p += 9;
            } else {
                numad_log(LOG_CRIT, "Could not get node MemTotal\n");
                exit(EXIT_FAILURE);
            }
            while (!isdigit(*p)) { p++; }
            CONVERT_DIGITS_TO_NUM(p, KB);
            node[node_ix].MBs_total = KB / KILOBYTE;
            p = strstr(p, "MemFree:");
            if (p != NULL) {
                p += 8;
            } else {
                numad_log(LOG_CRIT, "Could not get node MemFree\n");
                exit(EXIT_FAILURE);
            }
            while (!isdigit(*p)) { p++; }
            CONVERT_DIGITS_TO_NUM(p, KB);
            node[node_ix].MBs_free = KB / KILOBYTE;
            close(fd);
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
            int num_cpus_to_process = node[node_ix].CPUs_total / ONE_HUNDRED;
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
            if (node[node_ix].CPUs_free > node[node_ix].CPUs_total) {
                node[node_ix].CPUs_free = node[node_ix].CPUs_total;
            }
            node[node_ix].magnitude = node[node_ix].CPUs_free * node[node_ix].MBs_free;
        } else {
            node[node_ix].CPUs_free = 0;
            node[node_ix].magnitude = 0;
        }
    }
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
    uint64_t vsize;
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
    if (bytes < 50) {
        numad_log(LOG_WARNING, "Could not read stat file: %s\n", fname);
        return NULL;
    }
    close(fd);
    char *p = buf;
    static process_data_t data;
    // Get PID from field 0
    uint64_t val = *p++ - '0'; while (isdigit(*p)) { val *= 10; val += (*p++ - '0'); }
    data.pid = val;
    // Copy comm from field 1
    while (*p == ' ') { p++; }
    data.comm = p; while (*p != ' ') { p++; }
    *p++ = '\0';   // replacing the presumed single ' ' before next field
    // Skip fields 2 through 12
    for (int ix = 0;  (ix < 11);  ix++) { while (*p != ' ') { p++; } while (*p == ' ') { p++; } }
    // Get utime from field 13 for cpu_util
    val = *p++ - '0'; while (isdigit(*p)) { val *= 10; val += (*p++ - '0'); }
    data.cpu_util = val;
    // Get stime from field 14 to add on to cpu_util (which already has utime)
    while (*p == ' ') { p++; }
    val = *p++ - '0'; while (isdigit(*p)) { val *= 10; val += (*p++ - '0'); }
    data.cpu_util += val;
    // Skip fields 15 through 18
    while (*p == ' ') { p++; }
    for (int ix = 0;  (ix < 4);  ix++) { while (*p != ' ') { p++; } while (*p == ' ') { p++; } }
    // Get num_threads from field 19
    val = *p++ - '0'; while (isdigit(*p)) { val *= 10; val += (*p++ - '0'); }
    data.num_threads = val;
    // Skip fields 20 through 22
    while (*p == ' ') { p++; }
    for (int ix = 0;  (ix < 3);  ix++) { while (*p != ' ') { p++; } while (*p == ' ') { p++; } }
    // Get rss from field 23 to compute MBs_used
    val = *p++ - '0'; while (isdigit(*p)) { val *= 10; val += (*p++ - '0'); }
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



id_list_p pick_numa_nodes(int pid, int cpus, int mbs) {
    char buf[BUF_SIZE];
    char buf2[BUF_SIZE];
    if (log_level >= LOG_DEBUG) {
        numad_log(LOG_DEBUG, "PICK NODES FOR:  PID: %d,  CPUs %d,  MBs %d\n", pid, cpus, mbs);
    }
    int num_existing_mems = 0;
    static id_list_p existing_mems_list_p;
    CLEAR_LIST(existing_mems_list_p);
    static node_data_p tmp_node;
    static uint64_t *process_MBs;
    static uint64_t *saved_magnitude_for_node;
    static int process_MBs_num_nodes;
    uint64_t time_stamp = get_time_stamp();
    // For existing processes, get miscellaneous process specific details
    int pid_ix;
    process_data_p p = NULL;
    if ((pid > 0) && ((pid_ix = process_hash_lookup(pid)) >= 0)) {
        p = &process_hash_table[pid_ix];
        // Quick rejection if this process has interleaved memory, but recheck it once an hour...
#define MIN_DELAY_FOR_INTERLEAVE (3600 * ONE_HUNDRED)
        if (((p->flags & PROCESS_FLAG_INTERLEAVED) > 0)
          && (p->bind_time_stamp + MIN_DELAY_FOR_INTERLEAVE > time_stamp)) {
            if (log_level >= LOG_DEBUG) {
                numad_log(LOG_DEBUG, "Skipping evaluation because of interleaved memory.\n");
            }
            return NULL;
        }
        // Get cpuset name for this process, and existing mems binding, if any.
        char fname[FNAME_SIZE];
        snprintf(fname, FNAME_SIZE, "/proc/%d/cpuset", pid);
        FILE *fs = fopen(fname, "r");
        if (!fs) {
            numad_log(LOG_WARNING, "Tried to research PID %d cpuset, but it apparently went away.\n", p->pid);
            return NULL;  // Assume the process terminated?
        }
        if (!fgets(buf, BUF_SIZE, fs)) {
            numad_log(LOG_WARNING, "Tried to research PID %d cpuset, but it apparently went away.\n", p->pid);
	    fclose(fs);
            return NULL;  // Assume the process terminated?
        }
        fclose(fs);
        ELIM_NEW_LINE(buf);
        if ((!p->cpuset_name) || (strcmp(p->cpuset_name, buf))) {
            if (p->cpuset_name != NULL) {
                free(p->cpuset_name);
            }
            p->cpuset_name = strdup(buf);
        }
        if (log_level >= LOG_DEBUG) {
            numad_log(LOG_DEBUG, "CPUSET_NAME: %s\n", p->cpuset_name);
        }
        snprintf(fname, FNAME_SIZE, "%s%s/cpuset.mems", cpuset_dir, p->cpuset_name);
        fs = fopen(fname, "r");
        if ((fs) && (fgets(buf, BUF_SIZE, fs))) {
            fclose(fs);
            num_existing_mems = add_ids_to_list_from_str(existing_mems_list_p, buf);
            if (log_level >= LOG_DEBUG) {
                str_from_id_list(buf, BUF_SIZE, existing_mems_list_p);
                numad_log(LOG_DEBUG, "EXISTING CPUSET NODE LIST: %s\n", buf);
            }
        } 
        // If this process was just recently bound, enforce a minimum delay
        // period between repeated attempts to potentially move the memory.
        // FIXME: ?? might this retard appropriate process expansion too much?  
#define MIN_DELAY_FOR_REEVALUATION (30 * ONE_HUNDRED)
        if (p->bind_time_stamp + MIN_DELAY_FOR_REEVALUATION > time_stamp) {
            // Skip re-evaluation because we just did it recently.
            if (log_level >= LOG_DEBUG) {
                numad_log(LOG_DEBUG, "Skipping evaluation because done too recently.\n");
            }
            return NULL;
        }
        // Look for short cut because of duplicate bindings.  If we have bound
        // this process to the same nodes multiple times already, and the load
        // on those nodes still seems acceptable, skip the rest of this and
        // just return NULL to indicate no change needed.  FIXME: should figure
        // out what can change that would make a rebinding desirable (e.g. (1)
        // some process gets sub-optimal allocation on busy machine which
        // subsequently becomes less busy leaving disadvantaged process. (2)
        // node load imbalance, (3) any process split across nodes which should
        // fit within a single node.) For now, just expire the dup_bid_count
        // occasionally, which is a reasonably good mitigation.
        // So, check to see if we should decay the dup_bind_count...
#define DUP_BIND_TIME_OUT (300 * ONE_HUNDRED)
        if ((p->dup_bind_count > 0) && (p->bind_time_stamp + DUP_BIND_TIME_OUT < time_stamp)) {
            p->dup_bind_count -= 1;
        }
        // Now, look for short cut because of duplicate bindings
        if (p->dup_bind_count > 0) {
            int node_id = 0;
            int nodes_have_cpu = 1;
            int nodes_have_ram = 1;
            int n = num_existing_mems;
            int min_resource_pct = 100 - target_utilization;
            if (min_resource_pct < 5) {
                min_resource_pct = 5;
            }
            while (n) {
                if (ID_IS_IN_LIST(node_id, existing_mems_list_p)) {
                    nodes_have_cpu &= ((100 * node[node_id].CPUs_free / node[node_id].CPUs_total) >= (min_resource_pct));
                    nodes_have_ram &= ((100 * node[node_id].MBs_free  / node[node_id].MBs_total)  >= (min_resource_pct));
                    n -= 1;
                }
                node_id += 1;
            }
            if ((nodes_have_cpu) && (nodes_have_ram)) {
                if (log_level >= LOG_DEBUG) {
                    numad_log(LOG_DEBUG, "Skipping evaluation because of repeat binding\n");
                }
                return NULL;
            }
            if (log_level >= LOG_DEBUG) {
                numad_log(LOG_DEBUG, "Evaluated for skipping by repeat binding, but CPUS: %d, RAM: %d\n", nodes_have_cpu, nodes_have_ram);
            }
        }
        // Fourth, add up per-node memory in use by this process. This scanning
        // is expensive and should be minimized.  Also, old kernels dismantle
        // transparent huge pages while producing the numa_maps memory
        // information! 
        // Check to see if dynamic structures need to grow.
        if (process_MBs_num_nodes < num_nodes + 1) {
            process_MBs_num_nodes = num_nodes + 1;
            // The "+1 node" is for accumulating interleaved memory
            process_MBs = realloc(process_MBs, process_MBs_num_nodes * sizeof(uint64_t));
            tmp_node = realloc(tmp_node, num_nodes * sizeof(node_data_t) );
            saved_magnitude_for_node = realloc(saved_magnitude_for_node, num_nodes * sizeof(uint64_t));
            if ((process_MBs == NULL) || (tmp_node == NULL) || (saved_magnitude_for_node == NULL)) {
                numad_log(LOG_CRIT, "process_MBs realloc failed\n");
                exit(EXIT_FAILURE);
            }
        }
        memset(process_MBs, 0, process_MBs_num_nodes * sizeof(uint64_t));
        snprintf(fname, FNAME_SIZE, "/proc/%d/numa_maps", pid);
        fs = fopen(fname, "r");
        if (!fs) {
            numad_log(LOG_WARNING, "Tried to research PID %d numamaps, but it apparently went away.\n", p->pid);
            return NULL;  // Assume the process terminated
        }
        int process_has_interleaved_memory = 0;
        while (fgets(buf, BUF_SIZE, fs)) {
            int interleaved_memory = 0;
            uint64_t page_size = page_size_in_bytes;
            const char *delimiters = " \n";
            char *p = strtok(buf, delimiters);
            while (p) {
                if (!strncmp(p, "interleave", 10)) {
                    interleaved_memory = 1;
                    process_has_interleaved_memory = 1;
                } else if (!strcmp(p, "huge")) {
                    page_size = huge_page_size_in_bytes;
                } else if (*p++ == 'N') {
                    int node;
                    uint64_t pages;
                    CONVERT_DIGITS_TO_NUM(p, node);
                    if (*p++ != '=') {
                        numad_log(LOG_CRIT, "numa_maps node number parse error\n");
                        exit(EXIT_FAILURE);
                    }
                    CONVERT_DIGITS_TO_NUM(p, pages);
                    process_MBs[node] += (pages * page_size);
                    if (interleaved_memory) {
                        // sum interleaved quantity in "extra node"
                        process_MBs[num_nodes] += (pages * page_size);
                    }
                }
                // Get next token on the line
                p = strtok(NULL, delimiters);
            }
        }
        fclose(fs);
        for (int ix = 0;  (ix <= num_nodes);  ix++) {
            process_MBs[ix] /= MEGABYTE;
            if (log_level >= LOG_DEBUG) {
                numad_log(LOG_DEBUG, "PROCESS_MBs[%d]: %ld\n", ix, process_MBs[ix]);
            }
        }
        if (process_has_interleaved_memory) {
            // Mark this process as having interleaved memory, and stamp it as done.
            p->flags |= PROCESS_FLAG_INTERLEAVED;
            p->bind_time_stamp = get_time_stamp();
            if (log_level >= LOG_DEBUG) {
                numad_log(LOG_DEBUG, "Skipping evaluation because of interleaved memory.\n");
            }
            return NULL;
        }
    }  // end of existing PID conditional
    // Make a copy of node available resources array.  Add in info specific to
    // this process to equalize available resource quantities wrt locations of
    // resources already in use by this process.  Inflate the value of already
    // assigned memory by approximately 3/2, because moving memory is
    // expensive.  Average the amount of CPUs_free across the existing nodes
    // used, because the threads are free to move around in that domain.  After
    // calculating combined magnitude of available resources, bias the values
    // towards existing locations for this process.
    int target_using_all_nodes = 0;
    uint64_t node_CPUs_free_for_this_process = 0;
    memcpy(tmp_node, node, num_nodes * sizeof(node_data_t) );
    if (num_existing_mems > 0) {
        node_CPUs_free_for_this_process = cpus; // ?? Correct for utilization target inflation?
        int node_id = 0;
        int n = num_existing_mems;
        while (n) {
            if (ID_IS_IN_LIST(node_id, existing_mems_list_p)) {
                node_CPUs_free_for_this_process += tmp_node[node_id].CPUs_free;
                n -= 1;
            }
            node_id += 1;
        }
        // Divide to get average CPUs_free for the nodes in use by process
        node_CPUs_free_for_this_process /= num_existing_mems;
    }
    for (int ix = 0;  (ix < num_nodes);  ix++) {
        if (pid > 0) {
            tmp_node[ix].MBs_free  += ((process_MBs[ix] * 12) / 8);
        }
        if ((num_existing_mems > 0) && (ID_IS_IN_LIST(ix, existing_mems_list_p))) {
            tmp_node[ix].CPUs_free = node_CPUs_free_for_this_process;
        }
        if (tmp_node[ix].CPUs_free > tmp_node[ix].CPUs_total) {
            tmp_node[ix].CPUs_free = tmp_node[ix].CPUs_total;
        }
        if (log_level >= LOG_DEBUG) {
            numad_log(LOG_DEBUG, "PROCESS_CPUs[%d]: %ld\n", ix, tmp_node[ix].CPUs_free);
        }
        // Calculate magnitude as product of available CPUs and available MBs
        tmp_node[ix].magnitude = tmp_node[ix].CPUs_free * tmp_node[ix].MBs_free;
        // Bias combined magnitude towards already assigned nodes
        if (ID_IS_IN_LIST(ix, existing_mems_list_p)) {
            tmp_node[ix].magnitude *= 9;
            tmp_node[ix].magnitude /= 8;
        }
        // Save the current magnitudes
        saved_magnitude_for_node[ix] = tmp_node[ix].magnitude;
    }
    // OK, figure out where to get resources for this request.
    static id_list_p target_node_list_p;
    CLEAR_LIST(target_node_list_p);
    int prev_node_used = -1;
    // Continue to allocate more resources until request are met.
    // OK if not not quite all the CPU request is met.
    // FIXME: ?? Is the following too much CPU flexing?
    while ((mbs > 0) || (cpus > (tmp_node[0].CPUs_total / 4))) {
        if (log_level >= LOG_DEBUG) {
            numad_log(LOG_DEBUG, "MBs: %d,  CPUs: %d\n", mbs, cpus);
        }
        // Sort nodes by magnitude of available resources.  Note that
        // inter-node distances (to the previous node used) are factored into
        // the sort.
        for (int ij = 0;  (ij < num_nodes);  ij++) {
            int big_ix = ij;
            for (int ik = ij + 1;  (ik < num_nodes);  ik++) {
                uint64_t ik_dist = 1;
                uint64_t big_ix_dist = 1;
                if (prev_node_used >= 0) {
                    ik_dist = tmp_node[ik].distance[prev_node_used];
                    big_ix_dist = tmp_node[big_ix].distance[prev_node_used];
                }
                // Scale magnitude comparison by distances to previous node used...
                if ((tmp_node[big_ix].magnitude / big_ix_dist) < (tmp_node[ik].magnitude / ik_dist)) {
                    big_ix = ik;
                }
            }
            if (big_ix != ij) {
                node_data_t tmp;
                memcpy((void *)&tmp, (void *)&tmp_node[ij], sizeof(node_data_t) );
                memcpy((void *)&tmp_node[ij], (void *)&tmp_node[big_ix], sizeof(node_data_t) );
                memcpy((void *)&tmp_node[big_ix], (void *)&tmp, sizeof(node_data_t) );
            }
        }
        if (log_level >= LOG_DEBUG) {
            for (int ix = 0;  (ix < num_nodes);  ix++) {
                numad_log(LOG_DEBUG, "Sorted magnitude[%d]: %ld\n", tmp_node[ix].node_id, tmp_node[ix].magnitude);
            }
        }
        if (tmp_node[0].node_id == prev_node_used) {
            // Hmmm.  Looks like the best node for more resources, is also the
            // last one we used.  This is not going to make progress...  So
            // just punt and use everything.
            OR_LISTS(target_node_list_p, target_node_list_p, all_nodes_list_p);
            target_using_all_nodes = 1;
            break;
        }
        prev_node_used = tmp_node[0].node_id;
        ADD_ID_TO_LIST(tmp_node[0].node_id, target_node_list_p);
        if (log_level >= LOG_DEBUG) {
            str_from_id_list(buf,  BUF_SIZE, existing_mems_list_p);
            str_from_id_list(buf2, BUF_SIZE, target_node_list_p);
            numad_log(LOG_DEBUG, "Existing nodes: %s  Target nodes: %s\n", buf, buf2);
        }
        if (EQUAL_LISTS(target_node_list_p, all_nodes_list_p)) {
            // Apparently we must use all resource nodes...
            target_using_all_nodes = 1;
            break;
        }
#define MBS_MARGIN 10
        if (tmp_node[0].MBs_free >= (mbs + MBS_MARGIN)) {
            tmp_node[0].MBs_free -= mbs;
            mbs = 0;
        } else {
            mbs -= (tmp_node[0].MBs_free - MBS_MARGIN);
            tmp_node[0].MBs_free = MBS_MARGIN;
        }
#define CPUS_MARGIN 0
        if (tmp_node[0].CPUs_free >= (cpus + CPUS_MARGIN)) {
            tmp_node[0].CPUs_free -= cpus;
            cpus = 0;
        } else {
            cpus -= (tmp_node[0].CPUs_free - CPUS_MARGIN);
            tmp_node[0].CPUs_free = CPUS_MARGIN;
        }
        tmp_node[0].magnitude = tmp_node[0].CPUs_free * tmp_node[0].MBs_free;
    }
    // If this existing process is already located where we want it, and almost
    // all memory is already moved to those nodes, then return NULL indicating
    // no need to change binding this time.
    if ((pid > 0) && (EQUAL_LISTS(target_node_list_p, existing_mems_list_p))) {
        // May not need to change binding.  However, if there is any significant
        // memory still on non-target nodes, advise the bind anyway because
        // there are some scenarios when the kernel will not move it all the
        // first time.
        if (!target_using_all_nodes) {
            p->dup_bind_count += 1;
            for (int ix = 0;  (ix < num_nodes);  ix++) {
                if ((process_MBs[ix] > 10) && (!ID_IS_IN_LIST(ix, target_node_list_p))) {
                    goto try_memory_move_again;
                }
            }
            // We will accept these memory locations.  Stamp it as done.
            p->bind_time_stamp = get_time_stamp();
        }
        // Skip rebinding either because practically all memory is in the
        // target nodes, or because we are stuck using all the nodes.
        if (log_level >= LOG_DEBUG) {
            numad_log(LOG_DEBUG, "Skipping evaluation because memory is reasonably situated.\n");
        }
        return NULL;
    } else {
        // Either a non-existing process, or a new binding for an existing process.
        if (p != NULL) {
            // Must be a new binding for an existing process, so reset dup_bind_count.
            p->dup_bind_count = 0;
        }
    }
    // See if this proposed move will make a significant difference.
    // If not, return null instead of advising the move.
    uint64_t target_magnitude = 0;
    uint64_t existing_magnitude = 0;
    int num_target_nodes   = NUM_IDS_IN_LIST(target_node_list_p);
    int num_existing_nodes = NUM_IDS_IN_LIST(existing_mems_list_p);
    /* FIXME: this expansion seems to cause excessive growth
     * So calculate the improvement before hastily expanding nodes.
    if (num_target_nodes > num_existing_nodes) { goto try_memory_move_again; }
    */
    int node_id = 0;
    int n = num_existing_nodes + num_target_nodes;
    while (n) {
        if (ID_IS_IN_LIST(node_id, target_node_list_p)) {
            target_magnitude += saved_magnitude_for_node[node_id];
            n -= 1;
        }
        if (ID_IS_IN_LIST(node_id, existing_mems_list_p)) {
            existing_magnitude += saved_magnitude_for_node[node_id];
            n -= 1;
        }
        node_id += 1;
    }
    if (existing_magnitude > 0) {
        uint64_t magnitude_change = ((target_magnitude - existing_magnitude) * 100) / existing_magnitude;
        if (magnitude_change < 0) {
            magnitude_change = -(magnitude_change);
        }
        if (magnitude_change <= IMPROVEMENT_THRESHOLD_PERCENT) {
            // Not significant enough percentage change to do rebind
            if (log_level >= LOG_DEBUG) {
                str_from_id_list(buf,  BUF_SIZE, existing_mems_list_p);
                str_from_id_list(buf2, BUF_SIZE, target_node_list_p);
                numad_log(LOG_DEBUG, "Moving pid %d from nodes (%s) to nodes (%s) skipped as insignificant improvement: %ld percent.\n",
                    pid, buf, buf2, magnitude_change);
            }
            // We decided this is almost good enough.  Stamp it as done.
            p->bind_time_stamp = get_time_stamp();
            return NULL;
        }
    }
try_memory_move_again:
    str_from_id_list(buf,  BUF_SIZE, existing_mems_list_p);
    str_from_id_list(buf2, BUF_SIZE, target_node_list_p);
    char *cmd_name = "(unknown)";
    if ((p) && (p->comm)) {
        cmd_name = p->comm;
    }
    numad_log(LOG_NOTICE, "Advising pid %d %s move from nodes (%s) to nodes (%s)\n", pid, cmd_name, buf, buf2);
    return target_node_list_p;
}



void show_processes(process_data_p *ptr, int nprocs) {
    time_t ts = time(NULL);
    fprintf(log_fs, "%s", ctime(&ts));
    fprintf(log_fs, "Candidates: %d\n", nprocs);
    for (int ix = 0;  (ix < nprocs);  ix++) {
        process_data_p p = ptr[ix];
        char buf[BUF_SIZE];
        snprintf(buf, BUF_SIZE, "%s%s/cpuset.mems", cpuset_dir, p->cpuset_name);
        FILE *fs = fopen(buf, "r");
        buf[0] = '\0';
        if (fs) {
            if (fgets(buf, BUF_SIZE, fs)) {
                ELIM_NEW_LINE(buf);
            }
            fclose(fs);
        }
        fprintf(log_fs, "%ld: PID %d: %s, Threads %2ld, MBs_used %6ld, CPUs_used %4ld, Magnitude %6ld, Nodes: %s\n", 
            p->data_time_stamp, p->pid, p->comm, p->num_threads, p->MBs_used, p->CPUs_used, p->MBs_used * p->CPUs_used, buf);
        }
    fprintf(log_fs, "\n");
    fflush(log_fs);
}



int manage_loads() {
    // Use temporary index to access and sort hash table entries
    static process_data_p *pindex;
    static int pindex_size;
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
    // Copy live candidate pointers to the index for sorting, etc
    int nprocs = 0;
    for (int ix = 0;  (ix < process_hash_table_size);  ix++) {
        process_data_p p = &process_hash_table[ix];
        if (p->pid) {
            pindex[nprocs++] = p;
        }
    }
    // Sort index by amount of CPU used * amount of memory used.  Not expecting
    // a long list here.  Use a simple sort -- however, sort into bins,
    // treating values within 10% as aquivalent.  Within bins, order by
    // bind_time_stamp so oldest bound will be higher priority to evaluate.
    for (int ij = 0;  (ij < nprocs);  ij++) {
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
            if ((diff_mag > 0) && (min_mag / diff_mag < 10)) {
                // difference > 10 percent.  Use strict ordering
                if (ik_mag <= best_mag) continue;
            } else {
                // difference within 10 percent.  Sort these by bind_time_stamp.
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
    if ((log_level >= LOG_INFO) && (nprocs > 0)) {
        show_processes(pindex, nprocs);
    }
    // Estimate desired size and make resource requests for each significant process
    for (int ix = 0;  (ix < nprocs);  ix++) {
        process_data_p p = pindex[ix];
        if (p->CPUs_used * p->MBs_used < CPU_THRESHOLD * MEMORY_THRESHOLD) {
            break; // No more significant processes worth worrying about...
        }
        int mb_request  =  (p->MBs_used * 100) / target_utilization;
        int cpu_request = (p->CPUs_used * 100) / target_utilization;
        // Do not give a process more CPUs than it has threads!
        // FIXME: For guest VMs, should limit max to VCPU threads. Will
        // need to do something more intelligent with guest IO threads
        // when eventually considering devices and IRQs.
        int thread_limit = p->num_threads;
        // If process looks like a KVM guest, try to limit to number of vCPU threads
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
        pthread_mutex_lock(&node_info_mutex);
        id_list_p node_list_p = pick_numa_nodes(p->pid, cpu_request, mb_request);
        // FIXME: ?? copy node_list_p to shorten mutex region?
        if ((node_list_p != NULL) && (bind_process_and_migrate_memory(p->pid, p->cpuset_name, node_list_p, NULL))) {
            // Shorten interval if actively moving processes
            pthread_mutex_unlock(&node_info_mutex);
            p->bind_time_stamp = get_time_stamp();
            return min_interval;
        }
        pthread_mutex_unlock(&node_info_mutex);
    }
    // Return maximum interval if no process movement
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
	case 'i':
	    min_interval = msg.body.arg1;
	    max_interval = msg.body.arg2;
	    if (max_interval <= 0) {
		shut_down_numad();
	    }
	    numad_log(LOG_NOTICE, "Changing interval to %d:%d\n", msg.body.arg1, msg.body.arg2);
	    break;
	case 'l':
	    numad_log(LOG_NOTICE, "Changing log level to %d\n", msg.body.arg1);
	    log_level = msg.body.arg1;
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
	case 'u':
	    numad_log(LOG_NOTICE, "Changing target utilization to %d\n", msg.body.arg1);
	    target_utilization = msg.body.arg1;
	    break;
	case 'w':
	    numad_log(LOG_NOTICE, "Getting NUMA pre-placement advice for %d CPUs and %d MBs\n",
                                    msg.body.arg1, msg.body.arg2);
	    pthread_mutex_lock(&node_info_mutex);
	    update_nodes();
	    id_list_p node_list_p = pick_numa_nodes(-1, (msg.body.arg1 * ONE_HUNDRED), msg.body.arg2);
	    str_from_id_list(buf, BUF_SIZE, node_list_p);
	    pthread_mutex_unlock(&node_info_mutex);
	    send_msg(msg.body.src_pid, 'w', requested_cpus, requested_mbs, buf);
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



void parse_two_arg_values(char *p, int *first_ptr, int *second_ptr, int first_is_optional) {
    char *orig_p = p;
    char *q = NULL;
    int second = -1;
    int first = (int)strtol(p, &p, 10);
    if (p == orig_p) {
        fprintf(stderr, "Can't parse arg value(s): %s\n", orig_p);
        exit(EXIT_FAILURE);
    }
    if (*p == ':') {
        q = p + 1;
        second = (int)strtol(q, &p, 10);
        if (p == q) {
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
    int d_flag = 0;
    int i_flag = 0;
    int l_flag = 0;
    int p_flag = 0;
    int r_flag = 0;
    int S_flag = 0;
    int u_flag = 0;
    int v_flag = 0;
    int w_flag = 0;
    int x_flag = 0;
    long list_pid = 0;
    while ((opt = getopt(argc, argv, "dD:hi:l:p:r:S:u:vVw:x:")) != -1) {
        switch (opt) {
        case 'd':
            d_flag = 1;
            log_level = LOG_DEBUG;
            break;
        case 'D':
            cpuset_dir_list[0] = strdup(optarg);
            break;
        case 'h':
            print_usage_and_exit(argv[0]);
            break;
        case 'i':
            i_flag = 1;
            parse_two_arg_values(optarg, &min_interval, &max_interval, 1);
            break;
        case 'l':
            l_flag = 1;
            log_level = atoi(optarg);
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
        case 'S':
            S_flag = 1;
            scan_all_processes = (atoi(optarg) != 0);
            break;
        case 'u':
            u_flag = 1;
            target_utilization = atoi(optarg);
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
            parse_two_arg_values(optarg, &requested_cpus, &requested_mbs, 0);
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
        if (i_flag) {
            send_msg(daemon_pid, 'i', min_interval, max_interval, "");
        }
        if (d_flag || l_flag || v_flag) {
            send_msg(daemon_pid, 'l', log_level, 0, "");
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
    } else if (w_flag) {
        // Get pre-placement NUMA advice without starting daemon
        char buf[BUF_SIZE];
        update_nodes();
        sleep(2);
        update_nodes();
        numad_log(LOG_NOTICE, "Getting NUMA pre-placement advice for %d CPUs and %d MBs\n", requested_cpus, requested_mbs);
        id_list_p node_list_p = pick_numa_nodes(-1, (requested_cpus * ONE_HUNDRED), requested_mbs);
        str_from_id_list(buf, BUF_SIZE, node_list_p);
        fprintf(stdout, "%s\n", buf);
        close_log_file();
        exit(EXIT_SUCCESS);
    } else if (max_interval > 0) {
        // Start the numad daemon...
        check_prereqs(argv[0]);
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
        // Allocate initial process hash table
        process_hash_table_expand();
        // Spawn thread to handle messages from subsequent invocation requests
        pthread_mutex_init(&pid_list_mutex, NULL);
        pthread_mutex_init(&node_info_mutex, NULL);
        pthread_attr_t attr;
        if (pthread_attr_init(&attr) != 0) {
            numad_log(LOG_CRIT, "pthread_attr_init failure\n");
            exit(EXIT_FAILURE);
        }
        pthread_t tid;
        if (pthread_create(&tid, &attr, &set_dynamic_options, &tid) != 0) {
            numad_log(LOG_CRIT, "pthread_create failure\n");
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
            }
            sleep(interval);
        }
        if (pthread_attr_destroy(&attr) != 0) {
            numad_log(LOG_WARNING, "pthread_attr_destroy failure\n");
        }
        pthread_mutex_destroy(&pid_list_mutex);
        pthread_mutex_destroy(&node_info_mutex);
    } else {
        shut_down_numad();
    }
    exit(EXIT_SUCCESS);
}

