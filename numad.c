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



// Compile with: gcc -O -std=gnu99 -pthread -o numad numad.c 




#if 0
/*

TODO:
=====

- fix issues
- verify args
- inclusion / exclusion lists, per-process ???force??? flags
- add option to self-calculate SLIT distance info
- extend dynamic intervals?
- hi / lo watermarks, and ring buffer for processor cpu_used
- write some internal documentation, explaining purposes and design
- add a lazy thread that slowly cleans up obsolete cpusets
- testing and verification

*/
#endif




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



#define VERSION_STRING "20120305b"



#define VAR_RUN_FILE "/var/run/numad.pid"
#define VAR_LOG_FILE "/var/log/numad.log"

char *cpuset_dir = NULL;
char *cpuset_dir_list[] =  {
    NULL,
    "/sys/fs/cgroup/cpuset",
    "/cgroup/cpuset",
    NULL
};



#define QEMU_CPUSET_SUBDIR "libvirt/qemu"



#define MAX_NODES 256

#define MAX_CPUS  2048


#define KILOBYTE (1024)
#define MEGABYTE (1024 * 1024)
#define GIGABYTE (1024 * 1024 * 1024)

#define BUF_SIZE 1024
#define FNAME_SIZE 192

#define CPU_SCALE_FACTOR 100

#define CPU_THRESHOLD    30
#define MEMORY_THRESHOLD 300

#define MIN_INTERVAL  5
#define MAX_INTERVAL 15
#define TARGET_UTILIZATION_PERCENT 85


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
int cur_cpu_data_buf = 0;
int page_size_in_bytes = 0;
int huge_page_size_in_bytes = 0;

int min_interval = MIN_INTERVAL;
int max_interval = MAX_INTERVAL;
int target_utilization  = TARGET_UTILIZATION_PERCENT;

pthread_mutex_t mutex;
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



#define MSG_BODY_TEXT_SIZE 48

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
    strcpy(msg.body.text, s);
    size_t len = sizeof(msg_body_t) - MSG_BODY_TEXT_SIZE + strlen(msg.body.text) + 1;
    if (msgsnd(msg_qid, &msg, len, IPC_NOWAIT) < 0) {
        numad_log(LOG_CRIT, "msgsnd failed\n");
        exit(EXIT_FAILURE);
    }
    // printf("Sent: >>%s<< to process %d\n", msg.body.text, msg.dst_pid);
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
    fprintf(stderr, "-h to print this usage info\n");
    fprintf(stderr, "-i [<MIN>:]<MAX> to specify interval seconds\n");
    fprintf(stderr, "-l <N> to specify logging level (usually 5, 6, or 7)\n");
    fprintf(stderr, "-u <N> to specify target utilization percent\n");
    fprintf(stderr, "-v for verbose  (same effect as '-l 6'\n");
    fprintf(stderr, "-V to show version info\n");
    fprintf(stderr, "-w <CPUs>[:<MBs>] for node suggestions\n");
    exit(EXIT_FAILURE);
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
            if (*s == '\0') {
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


id_list_p all_cpus_list_p;
id_list_p all_nodes_list_p;






typedef struct node_data {
    uint64_t node_id;
    uint64_t MBs_total;
    uint64_t MBs_free;
    uint64_t CPUs_total; // scaled * CPU_SCALE_FACTOR
    uint64_t CPUs_free;  // scaled * CPU_SCALE_FACTOR
    uint64_t magnitude;  // hack: MBs * CPUs
    uint8_t distance[MAX_NODES];
    id_list_p cpu_list_p; 
} node_data_t, *node_data_p;

node_data_t node[MAX_NODES];

int node_from_cpu[MAX_CPUS];


typedef struct per_cpu_stats_data {
    // Statistics from /proc/stat cpu<N> lines
    // (assuming at least 2.6.24 kernel)
    uint64_t user;
    uint64_t nice;
    uint64_t system;
    uint64_t idle;
    uint64_t iowait;
    uint64_t irq;
    uint64_t softirq;
    uint64_t steal;
    uint64_t guest;
} per_cpu_stats_data_t, *per_cpu_stats_data_p;

typedef struct cpu_data {
    // stats from /proc/uptime
    uint64_t uptime;   // scaled * CPU_SCALE_FACTOR
    uint64_t sys_idle; // scaled * CPU_SCALE_FACTOR
    // stats from /proc/stat, cpu<N> lines
    per_cpu_stats_data_t cpu_stats[MAX_CPUS];
} cpu_data_t, *cpu_data_p;

cpu_data_t cpu_data_buf[2];  // Two sets, to calc deltas


typedef struct stat_data {
    int pid;
    char *comm;
    int state;
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
    uint64_t utime;
    uint64_t stime;
    int64_t cutime;
    int64_t cstime;
    int64_t priority;
    int64_t nice;
    int64_t num_threads;
    int64_t itrealvalue;
    uint64_t starttime;
    uint64_t vsize;
    int64_t rss;
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
    unsigned policy;
    uint64_t delayacct_blkio_ticks;
    uint64_t guest_time;
    int64_t cguest_time;
} stat_data_t, *stat_data_p;

#define RING_BUF_SIZE 16
#define RING_BUF_MASK 0xf

typedef struct process_data {
    // Most process stats are derived from /proc/<PID>/stat info -- subset of
    // stat_data above.  Currently use only about half of these...
    int pid;
    // int pgrp;
    char *comm;
    uint64_t utime;
    uint64_t stime;
    // int64_t priority;
    // int64_t nice;
    int64_t num_threads;
    // uint64_t starttime;
    // uint64_t vsize_MBs;
    uint64_t rss_MBs;
    int processor;
    // unsigned rt_priority;
    // unsigned policy;
    // uint64_t guest_time;
    uint64_t uptime;    // Data timestamp from /proc/uptime
    char *cpuset_name;
    uint64_t MBs_used;
    uint64_t CPUs_used;
    uint64_t CPUs_used_ring_buf[RING_BUF_SIZE];
    int ring_buf_ix;
    int dup_bind_count;
    // id_list_p node_list_p; 
} process_data_t, *process_data_p;

#define PROCESS_HASH_TABLE_SIZE 2003
process_data_t process_hash_table[PROCESS_HASH_TABLE_SIZE];

int process_hash_collisions = 0;


int process_hash_ix(int pid) {
    return (pid % PROCESS_HASH_TABLE_SIZE);
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
        if (ix >= PROCESS_HASH_TABLE_SIZE) {
            ix = 0;
        }
        if (ix == starting_ix) {
            break;  // table full and pid not found
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
        ix += 1;
        if (ix >= PROCESS_HASH_TABLE_SIZE) {
            ix = 0;
        }
        process_hash_collisions += 1;
        if (ix == starting_ix) {
            numad_log(LOG_ERR, "Process hash table is full\n");
            // FIXME: do something here, or preferrably much sooner, 
            // perhaps when some collisions threshold is passed
            return -1;
        }
    }
    process_hash_table[ix].pid = pid;
    return ix;
}

int process_hash_update(process_data_p newp) {
    // This updates hash table stats for processes we are monitoring
    int ix = process_hash_insert(newp->pid);
    if (ix >= 0) {
        process_data_p p = &process_hash_table[ix];
        p->MBs_used = newp->rss_MBs;
        if (p->uptime) {
            p->ring_buf_ix += 1;
            p->ring_buf_ix &= RING_BUF_MASK;
            uint64_t  utime_diff = newp->utime  - p->utime;
            uint64_t  stime_diff = newp->stime  - p->stime;
            uint64_t uptime_diff = newp->uptime - p->uptime;
            p->CPUs_used_ring_buf[p->ring_buf_ix] = 100 * (utime_diff + stime_diff) / uptime_diff;
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
        // p->pgrp = newp->pgrp;
        // p->nice = newp->nice;
        // p->policy = newp->policy;
        // p->priority = newp->priority;
        p->processor = newp->processor;
        p->rss_MBs = newp->rss_MBs;
        // p->vsize_MBs = newp->vsize_MBs;
        // p->rt_priority = newp->rt_priority;
        // p->starttime = newp->starttime;
        p->stime = newp->stime;
        p->num_threads = newp->num_threads;
        p->uptime = newp->uptime;
        p->utime = newp->utime;
        // p->guest_time = newp->guest_time;
    }
    return ix;
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
        while (pid = process_hash_table[++ix].pid) {
            if (process_hash_lookup(pid) < 0) {
                if (process_hash_rehash(ix) < 0) {
                    numad_log(LOG_ERR, "rehash fail\n");
                }
            }
        }
    }
    return ix;
}








void check_prereqs(char *prog_name) {
    // Verify cpusets are available on this system.
    char **dir = &cpuset_dir_list[0];
    if (*dir == NULL) { *dir++; }
    while (*dir != NULL) {
        cpuset_dir = *dir;
        char fname[FNAME_SIZE];
        snprintf(fname, FNAME_SIZE, "%s/cpuset.cpus", cpuset_dir);
        if (access(fname, F_OK) == 0) {
            break;
        }
        *dir++;
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
        if (*dir == NULL) { *dir++; }
        while (*dir != NULL) {
            fprintf(stderr, "      - %s\n", *dir);
            *dir++;
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
        }
        if (ms > 100) {
            fprintf(stderr, "\n");
            numad_log(LOG_NOTICE, "Looks like transparent hugepage scan time in %s is %d ms.\n", thp_scan_fname, ms);
            fprintf(stderr,       "Looks like transparent hugepage scan time in %s is %d ms.\n", thp_scan_fname, ms);
            fprintf(stderr, "Consider increasing the frequency of THP scanning,\n");
            fprintf(stderr, "by echoing a smaller number (e.g. 10) to %s\n", thp_scan_fname);
            fprintf(stderr, "to more agressively (re)construct THPs.  For example:\n");
            fprintf(stderr, "# echo 10 > /sys/kernel/mm/redhat_transparent_hugepage/khugepaged/scan_sleep_millisecs\n");
            fprintf(stderr, "\n");
            // For a similar tool, IBM monitored the cpu utilization of the khugepaged
            // kernel thread using a pidstat like utility and saw very low utilization
            // while running with this configuration (0.05%) while maintaining a high
            // ratio of anonymous huge pages / anonymous pages (99%).
        }
    }
}


int get_daemon_pid() {
    int fd = open(VAR_RUN_FILE, O_RDONLY, 0);
    if (fd < 0) {
        return 0;
    }
    char buf[BUF_SIZE];
    int bytes = read(fd, buf, BUF_SIZE);
    close(fd);
    if (bytes > 0) {
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


static __inline__ uint64_t rdtsc() {
    unsigned hi, lo;
    // __asm__ __volatile__ ( "cpuid" );
    // ?? use rdtscp on i7 (and later), instead of previous cpuid
    __asm__ __volatile__ ( "rdtsc" : "=a"(lo), "=d"(hi));
    return ( (uint64_t)lo) | ( ((uint64_t)hi) << 32);
}


int get_num_cpus() {
    int n = sysconf(_SC_NPROCESSORS_CONF);
    int n2 = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < n2) {
        n = n2;
    }
    return n;
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


uint64_t get_uptime(uint64_t *idle_ptr) {
    assert(sysconf(_SC_CLK_TCK) == CPU_SCALE_FACTOR);
    // Return scaled system uptime info from /proc/uptime
    // If non-null idle_ptr provided, get system idle time too.
    static int fd = -1;
    if (fd >= 0) {
        // rewind the file
        lseek(fd, 0, SEEK_SET);
    } else {
        fd = open("/proc/uptime", O_RDONLY, 0);
        if (fd < 0) {
            goto fail_proc_uptime;
        }
    }
    char buf[BUF_SIZE];
    int bytes = read(fd, buf, BUF_SIZE);
    if (bytes <= 0) {
        goto fail_proc_uptime;
    }
    char *p = buf;
    uint64_t tmpul[2];
    // Parse one or two numbers depending on whether or not caller wants system idle
    for (int ix = 0;  (ix < (1 + (idle_ptr != NULL)));  ix++) {
        // skip over non-digits
        while (!isdigit(*p)) {
            if (*p++ == '\0') {
                goto fail_proc_uptime;
            }
        }
        // convert consecutive digits to a number, ignoring decimal point thus scaling by 100
        tmpul[ix] = *p++ - '0';
        while (isdigit(*p)) {
            tmpul[ix] *= 10;
            tmpul[ix] += (*p++ - '0');
            if (*p == '.') {
                p++;  // Skip decimal point
            }
        }
    }
    if (idle_ptr != NULL) {
        *idle_ptr = tmpul[1];
    }
    return tmpul[0];
fail_proc_uptime:
    numad_log(LOG_CRIT, "Cannot get /proc/uptime contents\n");
    exit(EXIT_FAILURE);
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
    uint64_t t0 = get_uptime(NULL);
    // Write CPU IDs out to cpuset.cpus file
    char fname[FNAME_SIZE];
    char id_list_buf[BUF_SIZE];
    snprintf(fname, FNAME_SIZE, "%s/cpuset.cpus", cpuset_name);
    int fd = open(fname, O_WRONLY | O_TRUNC, 0);
    if (fd == -1) {
        numad_log(LOG_CRIT, "Could not open cpuset.cpus -- errno: %d\n", errno);
        return 0;
    }
    int len = str_from_id_list(id_list_buf, BUF_SIZE, cpu_list_p);
    write(fd, id_list_buf, len);
    close(fd);
    // Write node IDs out to cpuset.mems file
    snprintf(fname, FNAME_SIZE, "%s/cpuset.mems", cpuset_name);
    fd = open(fname, O_WRONLY | O_TRUNC, 0);
    if (fd == -1) {
        numad_log(LOG_CRIT, "Could not open cpuset.mems -- errno: %d\n", errno);
        return 0;
    }
    len = str_from_id_list(id_list_buf, BUF_SIZE, node_list_p);
    write(fd, id_list_buf, len);
    close(fd);
    // Write "1" out to cpuset.memory_migrate file
    snprintf(fname, FNAME_SIZE, "%s/cpuset.memory_migrate", cpuset_name);
    fd = open(fname, O_WRONLY | O_TRUNC, 0);
    if (fd == -1) {
        numad_log(LOG_CRIT, "Could not open cpuset.memory_migrate -- errno: %d\n", errno);
        return 0;
    }
    write(fd, "1", 1);
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
    }
    close(fd);

    uint64_t t1 = get_uptime(NULL);
    // Check pid still active
    snprintf(fname, FNAME_SIZE, "/proc/%d", pid);
    if (access(fname, F_OK) < 0) {
        numad_log(LOG_WARNING, "Could not migrate pid\n");
        return 0;  // Assume the process terminated
    }
    numad_log(LOG_NOTICE, "PID %d moved to node(s) %s in %d.%d seconds\n", pid, id_list_buf, (t1-t0)/100, (t1-t0)%100);
    return 1;
}






void show_nodes() {
    time_t ts = time(NULL);
    fprintf(log_fs, "%s", ctime(&ts));
    fprintf(log_fs, "Nodes: %d\n", num_nodes);
    for (int ix = 0;  (ix < num_nodes);  ix++) {
        fprintf(log_fs, "Node %d: MBs_total %ld, MBs_free %ld, CPUs_total %ld, CPUs_free %ld,  Distance: ", 
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


int update_cpu_data() {
    char buf[BUF_SIZE];
    FILE *fs = fopen("/proc/stat", "r");
    if (!fs) {
        goto fail_proc_stat;
    }
    int new = 1 - cur_cpu_data_buf;
    // First get new uptime and idle time
    cpu_data_buf[new].uptime = get_uptime(&cpu_data_buf[new].sys_idle);
    // Now get stats for each cpu
    while (fgets(buf, BUF_SIZE, fs)) {
#if (NEED_MORE_THAN_IDLE)
        if (strstr(buf, "cpu ") == buf) {
            // Skip system total line...
            continue;
        }
        int cpu_id;
        uint64_t tmpul[9]; // assume 9 fields for /proc/stat cpu<N> lines
        memset(tmpul, 0, sizeof(tmpul));
        if (9 <= sscanf(buf, "cpu%d %Lu %Lu %Lu %Lu %Lu %Lu %Lu %Lu %Lu",
        &cpu_id,   &tmpul[0], &tmpul[1], &tmpul[2], &tmpul[3],
        &tmpul[4], &tmpul[5], &tmpul[6], &tmpul[7], &tmpul[8])) {
            cpu_data_buf[new].cpu_stats[cpu_id].user    = tmpul[0];
            cpu_data_buf[new].cpu_stats[cpu_id].nice    = tmpul[1];
            cpu_data_buf[new].cpu_stats[cpu_id].system  = tmpul[2];
            cpu_data_buf[new].cpu_stats[cpu_id].idle    = tmpul[3];
            cpu_data_buf[new].cpu_stats[cpu_id].iowait  = tmpul[4];
            cpu_data_buf[new].cpu_stats[cpu_id].irq     = tmpul[5];
            cpu_data_buf[new].cpu_stats[cpu_id].softirq = tmpul[6];
            cpu_data_buf[new].cpu_stats[cpu_id].steal   = tmpul[7];
            cpu_data_buf[new].cpu_stats[cpu_id].guest   = tmpul[8];
        }
#else
        if ( (buf[0] == 'c') && (buf[1] == 'p') && (buf[2] == 'u') && (isdigit(buf[3])) ) {
            register char *p = &buf[3];
            int cpu_id = *p++ - '0'; while (isdigit(*p)) { cpu_id *= 10; cpu_id += (*p++ - '0'); }
            while (!isdigit(*p)) { p++; } while (isdigit(*p)) { p++; }  // skip user
            while (!isdigit(*p)) { p++; } while (isdigit(*p)) { p++; }  // skip nice
            while (!isdigit(*p)) { p++; } while (isdigit(*p)) { p++; }  // skip system
            while (!isdigit(*p)) { p++; }
            uint64_t idle = *p++ - '0'; while (isdigit(*p)) { idle *= 10; idle += (*p++ - '0'); }
            cpu_data_buf[new].cpu_stats[cpu_id].idle = idle;
        }
#endif
    }
    fclose(fs);
    cur_cpu_data_buf = new;
    return (cpu_data_buf[1 - new].uptime > 0);  // True if both buffers valid
fail_proc_stat:
    numad_log(LOG_CRIT, "Cannot get /proc/stat contents\n");
    exit(EXIT_FAILURE);
}


int update_nodes() {
    num_nodes = 0;
    CLEAR_LIST(all_cpus_list_p);
    CLEAR_LIST(all_nodes_list_p);
    // While counting num_nodes, get various data for each node
    for (;;) {
        char buf[BUF_SIZE];
        char fname[FNAME_SIZE];
        // Get all the CPU IDs in this node...  Read lines from node<N>/cpulist
        // file, and set the corresponding bits in the node cpu list.
        snprintf(fname, FNAME_SIZE, "/sys/devices/system/node/node%d/cpulist", num_nodes);
        FILE *fs = fopen(fname, "r");
        if ((!fs) || (!fgets(buf, BUF_SIZE, fs))) {
            break;  // Assume the only failure here will be the end of the node directories....
        } 
        fclose(fs);
        // Assign the node id
        if (num_nodes >= MAX_NODES) {
            numad_log(LOG_CRIT, "NODE table too small\n");
            exit(EXIT_FAILURE);
        }
        node[num_nodes].node_id = num_nodes;
        ADD_ID_TO_LIST(num_nodes, all_nodes_list_p);
        // get cpulist from the cpulist string
        CLEAR_LIST(node[num_nodes].cpu_list_p);
        int n = add_ids_to_list_from_str(node[num_nodes].cpu_list_p, buf);
        OR_LISTS(all_cpus_list_p, all_cpus_list_p, node[num_nodes].cpu_list_p);
        node[num_nodes].CPUs_total = n * CPU_SCALE_FACTOR;
        // Get distance vector of ACPI SLIT data from node<N>/distance file
        snprintf(fname, FNAME_SIZE, "/sys/devices/system/node/node%d/distance", num_nodes);
        fs = fopen(fname, "r");
        if (!fs) {
            numad_log(LOG_CRIT, "Unexpected fopen() failure while getting node distance data\n");
            exit(EXIT_FAILURE);
        } 
        int rnode = 0;
get_next_latf_line:
        while (fgets(buf, BUF_SIZE, fs)) {
            char *p = buf;
            for (;;) {
                // skip over non-digits
                while (!isdigit(*p)) {
                    if (*p++ == '\0') {
                        goto get_next_latf_line;
                    }
                }
                int latf;
                CONVERT_DIGITS_TO_NUM(p, latf);
                node[num_nodes].distance[rnode++] = latf;
            }
        }
        fclose(fs);
        // Get available memory info from node<N>/meminfo file
        snprintf(fname, FNAME_SIZE, "/sys/devices/system/node/node%d/meminfo", num_nodes);
        fs = fopen(fname, "r");
        if (!fs) {
            numad_log(LOG_CRIT, "Unexpected fopen() failure while getting node meminfo\n");
            exit(EXIT_FAILURE);
        } 
        int MemTotal = 0;
        int MemFree = 0;
get_next_mem_line:
        while (fgets(buf, BUF_SIZE, fs) && ((!MemTotal) || (!MemFree))) {
            uint64_t *MB_ptr = NULL;
            char *p = strstr(buf, "MemTotal");
            if (p) {
                MemTotal = 1;
                MB_ptr = &node[num_nodes].MBs_total;
            } else {
                p = strstr(buf, "MemFree");
                if (p) {
                    MemFree = 1;
                    MB_ptr = &node[num_nodes].MBs_free;
                } else {
                    goto get_next_mem_line;
                }
            }
            // skip over non-digits
            while (!isdigit(*p)) {
                if (*p++ == '\0') {
                    goto get_next_mem_line;
                }
            }
            uint64_t KB;
            CONVERT_DIGITS_TO_NUM(p, KB);
            *MB_ptr = KB / 1024;
        }
        fclose(fs);
        // Increment node number
        num_nodes += 1;
    }
    // Sum CPU idle data for each node to calculate current node available capacity
    if (update_cpu_data()) {
        for (int node_ix = 0;  (node_ix < num_nodes);  node_ix++) {
            uint64_t idle_ticks = 0;
            int old = 1 - cur_cpu_data_buf;
            int cpu = 0;
            int n = node[node_ix].CPUs_total / 100;
            while (n) {
                if (ID_IS_IN_LIST(cpu, node[node_ix].cpu_list_p)) {
                    node_from_cpu[cpu] = node_ix;  // construct cpu to node map
                    idle_ticks += cpu_data_buf[cur_cpu_data_buf].cpu_stats[cpu].idle - cpu_data_buf[old].cpu_stats[cpu].idle;
                    n -= 1;
                }
                cpu += 1;
            }
            uint64_t uptime_diff = cpu_data_buf[cur_cpu_data_buf].uptime - cpu_data_buf[old].uptime;
            // printf("Node: %d   CPUs: %d   Uptime diff %ld   Idle ticks %ld\n", node_ix, node[node_ix].CPUs_total, uptime_diff, idle_ticks);
            node[node_ix].CPUs_free = (100 * idle_ticks) / uptime_diff;
            if (node[node_ix].CPUs_free > node[node_ix].CPUs_total) {
                node[node_ix].CPUs_free = node[node_ix].CPUs_total;
            }
            node[node_ix].magnitude = node[node_ix].CPUs_free * node[node_ix].MBs_free;
        }
    }
    // FIXME: add code here to calculate a new capacity vector scaled more precisely by node distance
    if (log_level >= LOG_INFO) {
        show_nodes();
    }
    return num_nodes;
}




stat_data_p get_stat_data(char *fname) {
    FILE *fs = fopen(fname, "r");
    if (!fs) {
        numad_log(LOG_WARNING, "Could not open stat file: %s\n", fname);
        return NULL;
    }
    static char buf[BUF_SIZE];
    register char *p = fgets(buf, BUF_SIZE, fs);
    fclose(fs);
    if (!p) {
        numad_log(LOG_WARNING, "Could not read stat file: %s\n", fname);
        return NULL;
    }
    static stat_data_t data;
#if (NEED_ALL_STAT_DATA)
    static char comm_buf[BUF_SIZE];
    data.comm = comm_buf;
    int scanf_elements = sscanf(buf, 
        "%d %s %c %d %d %d %d %d %u %lu %lu %lu %lu %lu %lu %ld %ld "
        "%ld %ld %ld %ld %llu %lu %ld %lu %lu %lu %lu %lu %lu %lu %lu "
        "%lu %lu %lu %lu %lu %d %d %u %u %llu %lu %ld",
        &data.pid, data.comm, &data.state, &data.ppid, &data.pgrp,
        &data.session, &data.tty_nr, &data.tpgid, &data.flags, &data.minflt,
        &data.cminflt, &data.majflt, &data.cmajflt, &data.utime, &data.stime,
        &data.cutime, &data.cstime, &data.priority, &data.nice,
        &data.num_threads, &data.itrealvalue, &data.starttime, &data.vsize,
        &data.rss, &data.rsslim, &data.startcode, &data.endcode,
        &data.startstack, &data.kstkesp, &data.kstkeip, &data.signal,
        &data.blocked, &data.sigignore, &data.sigcatch, &data.wchan,
        &data.nswap, &data.cnswap, &data.exit_signal, &data.processor,
        &data.rt_priority, &data.policy, &data.delayacct_blkio_ticks,
        &data.guest_time, &data.cguest_time);
    if (scanf_elements < 43) {
        numad_log(LOG_WARNING, "Could not parse stat file\n");
        return NULL;
    }
#else
    // Just parse a select few of the data items
    data.pid = *p++ - '0'; while (isdigit(*p)) { data.pid *= 10; data.pid += (*p++ - '0'); }
    while (*p == ' ') { p++; }
    data.comm = p; while (*p != ' ') { p++; }
    *p++ = '\0';
    for (int ix = 0;  (ix < 11);  ix++) { while (*p != ' ') { p++; } while (*p == ' ') { p++; } }
    data.utime = *p++ - '0'; while (isdigit(*p)) { data.utime *= 10; data.utime += (*p++ - '0'); }
    while (*p == ' ') { p++; }
    data.stime = *p++ - '0'; while (isdigit(*p)) { data.stime *= 10; data.stime += (*p++ - '0'); }
    while (*p == ' ') { p++; }
    for (int ix = 0;  (ix < 4);  ix++) { while (*p != ' ') { p++; } while (*p == ' ') { p++; } }
    data.num_threads = *p++ - '0'; while (isdigit(*p)) { data.num_threads *= 10; data.num_threads += (*p++ - '0'); }
    while (*p == ' ') { p++; }
    for (int ix = 0;  (ix < 3);  ix++) { while (*p != ' ') { p++; } while (*p == ' ') { p++; } }
    data.rss = *p++ - '0'; while (isdigit(*p)) { data.rss *= 10; data.rss += (*p++ - '0'); }
    while (*p == ' ') { p++; }
    for (int ix = 0;  (ix < 14);  ix++) { while (*p != ' ') { p++; } while (*p == ' ') { p++; } }
    data.processor = *p++ - '0'; while (isdigit(*p)) { data.processor *= 10; data.processor += (*p++ - '0'); }
#endif
    return &data;
}


int update_processes() {
    // Scan /proc/<PID>/stat files for processes we should perhaps manage. For all
    // processes, evaluate whether or not they should be added to our hash
    // table short list of managed processes candidates.  If so, update the statistics,
    // time stamp and utilization numbers for the select processes in the hash table.
    // Then, go through all managed processes to prune out-of-date or dead ones.
    uint64_t this_update_time = get_uptime(NULL);
    struct dirent **namelist;
    int files = scandir("/proc", &namelist, name_starts_with_digit, NULL);
    if (files < 0) {
        numad_log(LOG_CRIT, "Could not open /proc\n");
        exit(EXIT_FAILURE);
    }
    if (log_level >= LOG_INFO) {
        numad_log(LOG_INFO, "Processes: %d\n", files);
    }
    for (int ix = 0;  (ix < files);  ix++) {
        stat_data_p sdata_p;
        char fname[FNAME_SIZE];
        snprintf(fname, FNAME_SIZE, "/proc/%s/stat", namelist[ix]->d_name);
        if ((sdata_p = get_stat_data(fname)) != NULL) {
            // See if this process significant enough to be managed on short list
            // FIXME: should not this first conditional be the same as magnitude test in manage_load()?
            if ( (sdata_p->rss * page_size_in_bytes > 150 * MEGABYTE)) {
                process_data_t pdata;
                pdata.uptime = get_uptime(NULL);  // get_uptime() soon after get_stat_data()
                // FIXME: Replace unconditional "1" below with more filters
                // and tests here as well as perhaps explicit process
                // inclusion and exclusion lists, before we decide to add
                // this process to our managed set of candidate processes.
                //
                // Should perhaps include all VM processes as well as any
                // other process with a non-default cpuset.
                //
                // printf("PID: %d %s %d %lu\n", sdata_p->pid, sdata_p->comm, sdata_p->num_threads, sdata_p->vsize);
                if (1) {
                    pdata.pid = sdata_p->pid;
                    // pdata.pgrp = sdata_p->pgrp;
                    pdata.comm = sdata_p->comm;
                    pdata.utime = sdata_p->utime;
                    pdata.stime = sdata_p->stime;
                    // pdata.priority = sdata_p->priority;
                    // pdata.nice = sdata_p->nice;
                    pdata.num_threads = sdata_p->num_threads;
                    // pdata.starttime = sdata_p->starttime;
                    // pdata.vsize_MBs = sdata_p->vsize / MEGABYTE;
                    pdata.rss_MBs = (sdata_p->rss * page_size_in_bytes) / MEGABYTE;
                    pdata.processor = sdata_p->processor;
                    // pdata.rt_priority = sdata_p->rt_priority;
                    // pdata.policy = sdata_p->policy;
                    // pdata.guest_time = sdata_p->guest_time;
                    process_hash_update(&pdata);
                }
            }
    
        }
    }
    // Prune out-of-date processes
    for (int ix = 0;  (ix < PROCESS_HASH_TABLE_SIZE);  ix++) {
        process_data_p p = &process_hash_table[ix];
        if (p->pid) {
            if (p->uptime < this_update_time) {
                p->CPUs_used = 0; // Zero old CPU utilization
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
                        if (rc == -1) {
                            numad_log(LOG_ERR, "bad cpuset rmdir\n");
                            // exit(EXIT_FAILURE);
                        }
                    }
                    process_hash_remove(p->pid);
                }
            }
        }
    }
    return files;
}






id_list_p pick_numa_nodes(int pid, int cpus, int mbs) {
    int pid_ix;
    process_data_p p = NULL;
    int cpuset_name_len = 0;
    static id_list_p existing_mems_list_p;
    uint64_t process_MBs[MAX_NODES];
    uint64_t process_CPUs[MAX_NODES];
    CLEAR_LIST(existing_mems_list_p);
    memset(process_MBs,  0, sizeof(process_MBs));
    memset(process_CPUs, 0, sizeof(process_CPUs));
    if (log_level >= LOG_DEBUG) {
        numad_log(LOG_DEBUG, "PICK NODES FOR:  PID: %d,  CPUs %d,  MBs %d\n", pid, cpus, mbs);
    }
    // For existing processes, get miscellaneous process specific details
    if ((pid > 0) && ((pid_ix = process_hash_lookup(pid)) >= 0)) {
        p = &process_hash_table[pid_ix];
        char buf[BUF_SIZE];
        char fname[FNAME_SIZE];
        // First get cpuset name for this process, and existing mems binding, if any.
        snprintf(fname, FNAME_SIZE, "/proc/%d/cpuset", pid);
        FILE *fs = fopen(fname, "r");
        if (!fs) {
            numad_log(LOG_WARNING, "Tried to research PID %d cpuset, but it apparently went away.\n", p->pid);
            return NULL;  // Assume the process terminated?
        }
        if (!fgets(buf, BUF_SIZE, fs)) {
            numad_log(LOG_WARNING, "Tried to research PID %d cpuset, but it apparently went away.\n", p->pid);
	    // FIXME: open file leak
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
        cpuset_name_len = strlen(p->cpuset_name);
        if (log_level >= LOG_DEBUG) {
            numad_log(LOG_DEBUG, "CPUSET_NAME: %s\n", p->cpuset_name);
        }

        int num_existing_mems = 0;
        snprintf(fname, FNAME_SIZE, "%s%s/cpuset.mems", cpuset_dir, p->cpuset_name);
        fs = fopen(fname, "r");
        if ((fs) && (fgets(buf, BUF_SIZE, fs))) {
            fclose(fs);
            num_existing_mems = add_ids_to_list_from_str(existing_mems_list_p, buf);
            if (log_level >= LOG_DEBUG) {
                // FIXME: just print buf w/o exercising conversion routines
                str_from_id_list(buf, BUF_SIZE, existing_mems_list_p);
                numad_log(LOG_DEBUG, "EXISTING NODE LIST: %s\n", buf);
            }
        } 

        // If we have bound this process to the same nodes multiple times
        // already, and the load on those nodes seems acceptable, skip the rest
        // of this and just return NULL to indicate no change needed.  FIXME:
        // figure out what else can change that should cause a rebinding (e.g.
        // (1) some process gets sub-optimal allocation on busy machine which
        // subsequently becomes less busy leaving disadvantaged process. (2)
        // node load imbalance, (3) any process split across nodes with should
        // fit within a single node.)
        if (p->dup_bind_count > 1) {
            int node_id = 0;
            int nodes_have_cpu = 1;
            int nodes_have_ram = 1;
            int n = num_existing_mems;
            while (n) {
                if (ID_IS_IN_LIST(node_id, existing_mems_list_p)) {
                    nodes_have_cpu &= ((100 * node[node_id].CPUs_free / node[node_id].CPUs_total) >= (100 - target_utilization));
                    nodes_have_ram &= ((100 * node[node_id].MBs_free  / node[node_id].MBs_total)  >= (100 - target_utilization));
                    n -= 1;
                }
                node_id += 1;
            }
            if ((nodes_have_cpu) && (nodes_have_ram)) {
                if (log_level >= LOG_DEBUG) {
                    numad_log(LOG_DEBUG, "Skipping evaluation because of repeat binding\n", pid, cpus, mbs);
                }
                return NULL;
            }
        }


// FIXME: this scanning is expensive and must be minimized


        // Second, add up per-node memory in use by this process
        snprintf(fname, FNAME_SIZE, "/proc/%d/numa_maps", pid);
        fs = fopen(fname, "r");
        if (!fs) {
            numad_log(LOG_WARNING, "Tried to research PID %d numamaps, but it apparently went away.\n", p->pid);
            return NULL;  // Assume the process terminated?
        }
        while (fgets(buf, BUF_SIZE, fs)) {
            uint64_t page_size = page_size_in_bytes;
            const char *delimiters = " \t\r\n";
            char *p = strtok(buf, delimiters);
            while (p) {
                if (!strcmp(p, "huge")) {
                    page_size = huge_page_size_in_bytes;
                } else if (p[0] == 'N') {
                    int node = (int)strtol(&p[1], &p, 10);
                    if (p[0] != '=') {
                        numad_log(LOG_CRIT, "numa_maps node number parse error\n");
                        exit(EXIT_FAILURE);
                    }
                    uint64_t pages = strtol(&p[1], &p, 10);
                    process_MBs[node] += (pages * page_size);
                }
                // Get next token on the line
                p = strtok(NULL, delimiters);
            }
        }
        for (int ix = 0;  (ix < num_nodes);  ix++) {
            process_MBs[ix] /= MEGABYTE;
            if (log_level >= LOG_DEBUG) {
                numad_log(LOG_DEBUG, "PROCESS_MBs[%d]: %ld\n", ix, process_MBs[ix]);
            }
        }
        fclose(fs);

        // Third, add up per-node CPUs recently used by this process
        snprintf(fname, FNAME_SIZE, "/proc/%d/task", pid);
        struct dirent **namelist;
        int files = scandir(fname, &namelist, name_starts_with_digit, NULL);
        if (files < 0) {
            numad_log(LOG_WARNING, "Tried to research PID %d tasks, but it apparently went away.\n", p->pid);
            return NULL;  // Assume the process terminated?
        }
        for (int ix = 0;  (ix < files);  ix++) {
            snprintf(fname, FNAME_SIZE, "/proc/%d/task/%s/stat", pid, namelist[ix]->d_name);
            stat_data_p sdata_p = get_stat_data(fname);
            if (sdata_p != NULL) {
                process_CPUs[node_from_cpu[sdata_p->processor]] += 1;
            }
        }
        for (int ix = 0;  (ix < num_nodes);  ix++) {
            // Assume average load per thread
            process_CPUs[ix] *= (p->CPUs_used / p->num_threads); 
            if (log_level >= LOG_DEBUG) {
                numad_log(LOG_DEBUG, "PROCESS_CPUs[%d]: %ld\n", ix, process_CPUs[ix]);
            }
        }
    }



    // Make a copy of node available resources array
    static node_data_p tmp_node; // FIXME: what if num_nodes changes after allocate
    if (tmp_node == NULL) {
        tmp_node = malloc(num_nodes * sizeof(node_data_t) );
        if (tmp_node == NULL) {
            numad_log(LOG_CRIT, "malloc failed\n");
            exit(EXIT_FAILURE);
        }
    }
    memcpy(tmp_node, node, num_nodes * sizeof(node_data_t) );

    // Add in the info specific to this process to equalize available resource
    // quantities wrt locations of resources already in use by this process
    for (int ix = 0;  (ix < num_nodes);  ix++) {
        tmp_node[ix].MBs_free  += process_MBs[ix];
        if (EQUAL_LISTS(existing_mems_list_p, all_nodes_list_p)) {
            // If not already bound, consider only available memory for now (by marking all CPUs free)
            tmp_node[ix].CPUs_free = tmp_node[ix].CPUs_total;;
	} else {
            // If already bound, consider existing location of CPUs
            tmp_node[ix].CPUs_free += process_CPUs[ix];
	}
        if (tmp_node[ix].CPUs_free > tmp_node[ix].CPUs_total) {
            tmp_node[ix].CPUs_free = tmp_node[ix].CPUs_total;
        }
        // Calculate magnitude clearly biased towards existing location of memory
        tmp_node[ix].magnitude = tmp_node[ix].CPUs_free * (tmp_node[ix].MBs_free + ((2 * process_MBs[ix]) / 3));
    }

    static id_list_p target_node_list_p;
    CLEAR_LIST(target_node_list_p);
    int prev_node = -1;

    // Allocate sufficient resources
    while ((mbs > 0) || (cpus > 20)) {

        // First, sort nodes by magnitude
        for (int ij = 0;  (ij < num_nodes);  ij++) {
            int big_ix = ij;
            for (int ik = ij;  (ik < num_nodes);  ik++) {
                if (tmp_node[big_ix].magnitude < tmp_node[ik].magnitude) {
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

        if (tmp_node[0].node_id == prev_node) {
            // Not going to make progress...  Just use everything
            OR_LISTS(target_node_list_p, target_node_list_p, all_nodes_list_p);
            break;
        }
        prev_node = tmp_node[0].node_id;

        ADD_ID_TO_LIST(tmp_node[0].node_id, target_node_list_p);
        if (EQUAL_LISTS(target_node_list_p, all_nodes_list_p)) {
            break;  // Apparently must use all resource nodes...
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


        // FIXME
        // adjust all the magnitudes by the distance in very sketchy way
        for (int ix = 1;  (ix < num_nodes);  ix++) {
            tmp_node[ix].magnitude /= tmp_node[0].distance[tmp_node[ix].node_id];
        }
    }

    // If this existing process is already located where we want it, and almost
    // all memory is already moved to those nodes, then return NULL indicating
    // no need to change binding this time.
    if ((pid > 0) && (EQUAL_LISTS(target_node_list_p, existing_mems_list_p))) {
        // May not need to change binding.  However, if there is any significant
        // memory still on non-target nodes, advise the bind anyway.
        p->dup_bind_count += 1;
        for (int ix = 0;  (ix < num_nodes);  ix++) {
            if ((process_MBs[ix] > 10) && (!ID_IS_IN_LIST(ix, target_node_list_p))) {
                goto advise_bind;
            }
        }
        return NULL;
    } else {
        if (p != NULL) {
            p->dup_bind_count = 0;
        }
    }

    char buf1[BUF_SIZE];
    char buf2[BUF_SIZE];

advise_bind:

    str_from_id_list(buf1, BUF_SIZE, existing_mems_list_p);
    str_from_id_list(buf2, BUF_SIZE, target_node_list_p);
    numad_log(LOG_NOTICE, "Advising pid %d move from nodes (%s) to nodes (%s)\n", pid, buf1, buf2);

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
        fprintf(log_fs, "PID %d: MBs_used %ld, CPUs_used %ld, Threads %ld, Uptime %ld, Name: %s, Nodes: %s\n", 
            p->pid, p->MBs_used, p->CPUs_used, p->num_threads, p->uptime, p->comm, buf);
        }
    fprintf(log_fs, "\n");
    fflush(log_fs);
}


int manage_loads() {
    // Use temporary index to access and sort process hash table
    process_data_p pindex[PROCESS_HASH_TABLE_SIZE];
    memset(pindex, 0, sizeof(pindex));
    // Copy live candidate pointers to the index for sorting, etc
    int nprocs = 0;
    for (int ix = 0;  (ix < PROCESS_HASH_TABLE_SIZE);  ix++) {
        process_data_p p = &process_hash_table[ix];
        if (p->pid) {
            pindex[nprocs++] = p;
        }
    }
    // Sort index by amount of CPU used * amount of memory used.
    // Not expecting a long list here.  So just use a simple sort.
    for (int ij = 0;  (ij < nprocs);  ij++) {
        int best = ij;
        for (int ik = ij;  (ik < nprocs);  ik++) {
            if ((pindex[ik]->CPUs_used * pindex[ik]->MBs_used)
                <= (pindex[best]->CPUs_used * pindex[best]->MBs_used)) continue;
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
        // Don't give a process more CPUs than it has threads
        int thread_limit = p->num_threads * CPU_SCALE_FACTOR;
        if (cpu_request > thread_limit) {
            cpu_request = thread_limit;
        }
        pthread_mutex_lock(&mutex);
        id_list_p node_list_p = pick_numa_nodes(p->pid, cpu_request, mb_request);
        // FIXME: copy node_list_p to shorten mutex region?
        if ((node_list_p != NULL) && (bind_process_and_migrate_memory(p->pid, p->cpuset_name, node_list_p, NULL))) {
            // Shorten interval if actively moving processes
            pthread_mutex_unlock(&mutex);
            return min_interval;
        }
        pthread_mutex_unlock(&mutex);
    }
    // Return maximum interval if no process movement
    return max_interval;
}




void *set_dynamic_options(void *arg) {
    int arg_value = *(int *)arg;
    for (;;) {
        // Loop here forever waiting for a msg to do something...
        msg_t msg;
        recv_msg(&msg);
        switch (msg.body.cmd) {
            case 'i': {
                numad_log(LOG_NOTICE, "Changing interval to %d:%d\n", msg.body.arg1, msg.body.arg2);
                min_interval = msg.body.arg1;
                max_interval = msg.body.arg2;
                if (max_interval <= 0) {
                    shut_down_numad();
                }
                break;
            }
            case 'l': {
                numad_log(LOG_NOTICE, "Changing log level to %d\n", msg.body.arg1);
                log_level = msg.body.arg1;
                break;
            }
            case 'u': {
                numad_log(LOG_NOTICE, "Changing target utilization to %d\n", msg.body.arg1);
                target_utilization = msg.body.arg1;
                break;
            }
            case 'w': {
                char buf[BUF_SIZE];
                numad_log(LOG_NOTICE, "Getting NUMA pre-placement advice for %d CPUs and %d MBs\n", msg.body.arg1, msg.body.arg2);
                pthread_mutex_lock(&mutex);
                id_list_p node_list_p = pick_numa_nodes(-1, (msg.body.arg1 * CPU_SCALE_FACTOR), msg.body.arg2);
                str_from_id_list(buf, BUF_SIZE, node_list_p);
                pthread_mutex_unlock(&mutex);
                send_msg(msg.body.src_pid, 'w', requested_cpus, requested_mbs, buf);
                break;
            }
            default: {
                numad_log(LOG_WARNING, "Unexpected msg command: %c %d %d %s from PID %d\n",
                        msg.body.cmd, msg.body.arg1, msg.body.arg1, msg.body.text, msg.body.src_pid);
                break;
            }
        }
    } // for (;;)
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
    char *p = NULL;
    int i_flag = 0;
    int l_flag = 0;
    int u_flag = 0;
    int w_flag = 0;
    while ((opt = getopt(argc, argv, "dD:hi:l:u:vVw:")) != -1) {
	switch (opt) {
	    case 'd': log_level = LOG_DEBUG ; break;
	    case 'D': cpuset_dir_list[0] = strdup(optarg); break;
	    case 'h': print_usage_and_exit(argv[0]); break;
	    case 'i': i_flag = 1; parse_two_arg_values(optarg, &min_interval, &max_interval, 1); break;
	    case 'l': l_flag = 1; log_level = atoi(optarg); break;
	    case 'u': u_flag = 1; target_utilization = atoi(optarg); break;
	    case 'v': log_level = LOG_INFO; break;
	    case 'V': print_version_and_exit(argv[0]); break;
            case 'w': w_flag = 1; parse_two_arg_values(optarg, &requested_cpus, &requested_mbs, 0); break;
	    default: print_usage_and_exit(argv[0]); break;
	}
    }
    if (argc > optind) {
	fprintf(stderr, "Unexpected arg = %s\n", argv[optind]);
	exit(EXIT_FAILURE);
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
        if (l_flag) {
            send_msg(daemon_pid, 'l', log_level, 0, "");
        }
        if (u_flag) {
            send_msg(daemon_pid, 'u', target_utilization, 0, "");
        }
        if (w_flag) {
            send_msg(daemon_pid, 'w', requested_cpus, requested_mbs, "");
            recv_msg(&msg);
            fprintf(stdout, "%s\n", msg.body.text);
        }
    } else if (w_flag) {
        // Get pre-placement NUMA advice without starting daemon
        char buf[BUF_SIZE];
        int nodes = update_nodes();
        sleep(2);
        nodes = update_nodes();
        numad_log(LOG_NOTICE, "Getting NUMA pre-placement advice for %d CPUs and %d MBs\n", requested_cpus, requested_mbs);
        id_list_p node_list_p = pick_numa_nodes(-1, (requested_cpus * CPU_SCALE_FACTOR), requested_mbs);
        str_from_id_list(buf, BUF_SIZE, node_list_p);
        fprintf(stdout, "%s\n", buf);
        close_log_file();
        exit(EXIT_SUCCESS);
    } else {
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

        // spawn thread to handle messages from subsequent invocation requests
        pthread_mutex_init(&mutex, NULL);
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
            pthread_mutex_lock(&mutex);
            int nodes = update_nodes();
            pthread_mutex_unlock(&mutex);
            if (nodes > 1) {
                update_processes();
                interval = manage_loads();
            }
            sleep(interval);
        }

        if (pthread_attr_destroy(&attr) != 0) {
            numad_log(LOG_WARNING, "pthread_attr_destroy failure\n");
        }
        pthread_mutex_destroy(&mutex);
    }

    exit(EXIT_SUCCESS);
}

