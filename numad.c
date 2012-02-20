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



// compile with: gcc -g -std=gnu99 -o numad numad.c 
// (will need to add pthreads in subsequent versions)



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
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <values.h>



#define VERSION_STRING "20120214"


#if ((FEDORA) || (RHEL7))
#   define CPUSET_DIR "/sys/fs/cgroup/cpuset"
#else
#   define CPUSET_DIR "/cgroup/cpuset"
#endif

#define QEMU_CPUSET_DIR "/cgroup/cpuset/libvirt/qemu"

#define VAR_RUN_FILE "/var/run/numad.pid"
#define VAR_LOG_FILE "/var/log/numad.log"


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

#define MIN_INTERVAL  2
#define MAX_INTERVAL 10
#define TARGET_UTILIZATION_PERCENT 90


#define ELIM_NEW_LINE(s) if (s[strlen(s) - 1] == '\n') s[strlen(s) - 1] = '\0'


int debug = 0;
int quiet = 0;
int verbose = 0;
int num_cpus = 0;
int num_nodes = 0;
int cur_cpu_data_buf = 0;
int page_size_in_bytes = 0;
int huge_page_size_in_bytes = 0;

int max_interval = MAX_INTERVAL;
int target_utilization  = TARGET_UTILIZATION_PERCENT;

int req_mbs = 0;
int req_cpus = 0;

FILE *log_fs = NULL;



typedef struct id_list {
    // Use CPU_SET(3) <sched.h> cpuset bitmasks,
    // but bundle size and pointer together
    // and genericize for both CPU and Node IDs
    cpu_set_t *set_p; 
    size_t bytes;
} id_list_t, *id_list_p;

#define INIT_ID_LIST(list_p) \
    list_p = malloc(sizeof(id_list_t)); \
    if (list_p == NULL) { perror("malloc"); exit(EXIT_FAILURE); } \
    list_p->set_p = CPU_ALLOC(num_cpus); \
    if (list_p->set_p == NULL) { perror("CPU_ALLOC"); exit(EXIT_FAILURE); } \
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
	perror("Cannot add to NULL list");
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
        // convert consecutive digits to an ID
        int id = *s++ - '0';
        while (isdigit(*s)) {
            id *= 10;
            id += (*s++ - '0');
        }
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
	perror("Bad string for ID listing");
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


typedef struct process_data {
    // Most process stats are derived from /proc/<PID>/stat info
    int pid;
    int pgrp;
    char *comm_name;
    uint64_t utime;
    uint64_t stime;
    int64_t priority;
    int64_t nice;
    int64_t num_threads;
    uint64_t starttime;
    uint64_t vsize_MBs;
    uint64_t rss_MBs;
    int processor;
    unsigned rt_priority;
    unsigned policy;
    uint64_t guest_time;
    // Miscellaneous other per-process data
    uint64_t uptime;    // Data timestamp from /proc/uptime
    uint64_t MBs_used;
    uint64_t CPUs_used;
    char *cpuset_name;
    int dup_bind_count;
    id_list_p node_list_p; 
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
            perror("Process hash table is full");
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
            uint64_t utime_diff = newp->utime - p->utime;
            uint64_t  stime_diff = newp->stime  - p->stime;
            uint64_t      time_diff = newp->uptime    - p->uptime;
            p->CPUs_used = 100 * (utime_diff + stime_diff) / time_diff;
        }
        if ((!p->comm_name) || (strcmp(p->comm_name, newp->comm_name))) {
            if (p->comm_name) {
                free(p->comm_name);
            }
            p->comm_name = strdup(newp->comm_name);
        }
        p->pgrp = newp->pgrp;
        p->nice = newp->nice;
        p->policy = newp->policy;
        p->priority = newp->priority;
        p->processor = newp->processor;
        p->rss_MBs = newp->rss_MBs;
        p->vsize_MBs = newp->vsize_MBs;
        p->rt_priority = newp->rt_priority;
        p->starttime = newp->starttime;
        p->stime = newp->stime;
        p->num_threads = newp->num_threads;
        p->uptime = newp->uptime;
        p->utime = newp->utime;
        p->guest_time = newp->guest_time;
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
        if (dp->comm_name)   { free(dp->comm_name); }
        if (dp->cpuset_name) { free(dp->cpuset_name); }
        if (dp->node_list_p) { FREE_LIST(dp->node_list_p); }
        memset(dp, 0, sizeof(process_data_t));
        // bubble up the collision chain
        while (pid = process_hash_table[++ix].pid) {
            if (process_hash_lookup(pid) < 0) {
                if (process_hash_rehash(ix) < 0) {
                    perror("rehash fail");
                }
            }
        }
    }
    return ix;
}






FILE *open_log_file() {
    log_fs = fopen(VAR_LOG_FILE, "a");
    return log_fs;
}


int numad_log(const char *fmt, ...) {
    if (log_fs == NULL) {
	if (open_log_file() == NULL) {
	    perror("Cannot open log file");
	    exit(EXIT_FAILURE);
	}
    }
    va_list ap;
    va_start(ap, fmt);
    int rc = vfprintf(log_fs, fmt, ap);
    va_end(ap);
    fflush(log_fs);
    return rc;
}


void close_log_file() {
    if (log_fs != NULL) {
        fclose(log_fs);
        log_fs = NULL;
    }
}


void shut_down_numad() {
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
    fprintf(stderr, "-d for debug\n");
    fprintf(stderr, "-h to print this usage info\n");
    fprintf(stderr, "-i <N> to specify <N> second max_interval\n");
    // fprintf(stderr, "-q for quiet\n");
    fprintf(stderr, "-u <N> to specify target utilization percent\n");
    fprintf(stderr, "-v for verbose\n");
    fprintf(stderr, "-V to show version info\n");
    fprintf(stderr, "-w <CPUs>[:<MBs>] for node suggestions\n");
    exit(EXIT_FAILURE);
}


void check_prereqs(char *prog_name) {
    // Verify cpusets are available on this system.
    if (access(CPUSET_DIR, F_OK) < 0) {
        fprintf(stderr, "\n");
        fprintf(stderr, "Are CPUSETs enabled on this system?\n");
        fprintf(stderr, "They are required for %s to function.\n", prog_name);
        fprintf(stderr, "Check manpage CPUSET(7). You might need to do something like:\n");
        fprintf(stderr, "    # mkdir %s\n", CPUSET_DIR);
        fprintf(stderr, "    # mount cgroup -t cgroup -o cpuset %s\n", CPUSET_DIR);
        fprintf(stderr, "and then try again...\n");
        fprintf(stderr, "\n");
        // FIXME: should provide cmd line option to specify alternate cpuset dir path
	exit(EXIT_FAILURE);
    }
    // Karl Rister <kmr@us.ibm.com> says: FYI, we ended up tuning khugepaged -- to
    // more aggressively re-build 2MB pages after VM memory migrations -- like this:
    // 
    // echo 10 > /sys/kernel/mm/redhat_transparent_hugepage/khugepaged/scan_sleep_millisecs
    // 
    // We monitored the cpu utilization of the khugepaged kernel thread using a
    // pidstat like utility and saw very low utilization while running with this
    // configuration (0.05%) while maintaining a high ratio of anonymous huge pages
    // / anonymous pages (99%).
    char *thp_scan_fname = "/sys/kernel/mm/redhat_transparent_hugepage/khugepaged/scan_sleep_millisecs";
    int fd = open(thp_scan_fname, O_RDONLY, 0);
    if (fd >= 0) {
        int ms;
        char buf[BUF_SIZE];
        int bytes = read(fd, buf, BUF_SIZE);
        close(fd);
        if (bytes > 0) {
            // convert consecutive digits to a number
            char *p = buf;
            ms = *p++ - '0';
            while (isdigit(*p)) {
                ms *= 10;
                ms += (*p++ - '0');
            }
        }
        if (ms > 100) {
            fprintf(stderr, "\n");
            fprintf(stderr, "Looks like transparent hugepage scan time in %s is %d ms.\n", thp_scan_fname, ms);
                  numad_log("Looks like transparent hugepage scan time in %s is %d ms.\n", thp_scan_fname, ms);
            fprintf(stderr, "Consider increasing the frequency of THP scanning,\n");
            fprintf(stderr, "by echoing a smaller number (e.g. 10) to %s\n", thp_scan_fname);
            fprintf(stderr, "to more agressively (re)construct THPs.  For example:\n");
            fprintf(stderr, "# echo 10 > /sys/kernel/mm/redhat_transparent_hugepage/khugepaged/scan_sleep_millisecs\n");
            fprintf(stderr, "\n");
        }
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
        numad_log("Registering numad version %s PID %d\n", VERSION_STRING, pid);
        return 1;
    }
    if (errno == EEXIST) {
        fd = open(VAR_RUN_FILE, O_RDWR|O_CREAT,        S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
        if (fd < 0) {
            goto fail_numad_run_file;
        }
        int bytes = read(fd, buf, BUF_SIZE);
        close(fd);
        if (bytes > 0) {
            // convert consecutive digits to a number
            char *p = buf;
            pid = *p++ - '0';
            while (isdigit(*p)) {
                pid *= 10;
                pid += (*p++ - '0');
            }
            // Check pid in run file still active
            char fname[FNAME_SIZE];
            snprintf(fname, FNAME_SIZE, "/proc/%d", pid);
            if (access(fname, F_OK) < 0) {
                if (errno == ENOENT) {
                    // Assume run file is out-of-date...
                    numad_log("Removing out-of-date numad run file because %s doesn't exist\n", fname);
                    unlink(VAR_RUN_FILE);
                    goto create_run_file;
                }
            }
            // Daemon must be running already.
            return 0; 
        }

    }
fail_numad_run_file:
    perror("Cannot open numad.pid file");
    exit(EXIT_FAILURE);

}


void sigint_handler(int sig) {
}


static __inline__ uint64_t rdtsc() {
    unsigned hi, lo;
    // __asm__ __volatile__ ( "cpuid" );
    // ?? use rdtscp on i7 (and later), instead of previous cpuid
    __asm__ __volatile__ ( "rdtsc" : "=a"(lo), "=d"(hi));
    return ( (uint64_t)lo) | ( ((uint64_t)hi) << 32);
}


void bind_self_to_cpu(int cpu_id, size_t cpu_set_size, cpu_set_t *cpu_set_p) {
    CPU_ZERO_S(cpu_set_size, cpu_set_p);
    CPU_SET_S(cpu_id, cpu_set_size, cpu_set_p);
    if (sched_setaffinity(0, cpu_set_size, cpu_set_p) < 0) {
        perror("sched_setaffinity");
        exit(EXIT_FAILURE);
    }
    sched_yield();
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
	perror("Can't open /proc/meminfo");
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
        // convert consecutive digits to a number
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
    perror("Cannot get /proc/uptime contents");
    exit(EXIT_FAILURE);
}



static int name_starts_with_digit(const struct dirent *dptr) {
    return (isdigit(dptr->d_name[0]));
}







void show_nodes() {
    for (int ix = 0;  (ix < num_nodes);  ix++) {
        numad_log("Node %d: MBs_total %ld, MBs_free %ld, CPUs_total %ld, CPUs_free %ld,  Distance: ", 
            ix, node[ix].MBs_total, node[ix].MBs_free, node[ix].CPUs_total, node[ix].CPUs_free);
        for (int d = 0;  (d < num_nodes);  d++) {
            numad_log("%d ", node[ix].distance[d]);
        }
        char buf[BUF_SIZE];
        str_from_id_list(buf, BUF_SIZE, node[ix].cpu_list_p);
        numad_log(" CPUs: %s\n", buf);
    }
    numad_log("\n");
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
            char *p = &buf[3];
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
    perror("Cannot get /proc/stat contents");
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
            perror("NODE table too small");
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
            perror("Unexpected fopen() failure while getting node distance data");
            break;
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
                // convert consecutive digits to a latency factor
                int latf = *p++ - '0';
                while (isdigit(*p)) {
                    latf *= 10;
                    latf += (*p++ - '0');
                }
                node[num_nodes].distance[rnode++] = latf;
            }
        }
        fclose(fs);
        // Get available memory info from node<N>/meminfo file
        snprintf(fname, FNAME_SIZE, "/sys/devices/system/node/node%d/meminfo", num_nodes);
        fs = fopen(fname, "r");
        if (!fs) {
            perror("Unexpected fopen() failure while getting node meminfo");
            break;
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
            // convert consecutive digits to a number
            uint64_t KB = *p++ - '0';
            while (isdigit(*p)) {
                KB *= 10;
                KB += (*p++ - '0');
            }
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

    if (verbose) {
        show_nodes();
    }
    return num_nodes;
}


stat_data_p get_stat_data(char *fname) {
    static stat_data_t data;
    static char comm_buf[BUF_SIZE];
    data.comm = comm_buf;
    char buf[BUF_SIZE];
    FILE *fs = fopen(fname, "r");
    if ((!fs) || (!fgets(buf, BUF_SIZE, fs))) {
        numad_log("Could not read stat file: %s\n", fname);
        return NULL;
    }
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
        perror ("Could not parse stat file");
        return NULL;
    }
    fclose(fs);
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
        perror ("Could not open /proc");
    }
    if (verbose) {
        numad_log("Processes: %d\n", files);
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
                    pdata.pgrp = sdata_p->pgrp;
                    pdata.comm_name = sdata_p->comm;
                    pdata.utime = sdata_p->utime;
                    pdata.stime = sdata_p->stime;
                    pdata.priority = sdata_p->priority;
                    pdata.nice = sdata_p->nice;
                    pdata.num_threads = sdata_p->num_threads;
                    pdata.starttime = sdata_p->starttime;
                    pdata.vsize_MBs = sdata_p->vsize / MEGABYTE;
                    pdata.rss_MBs = (sdata_p->rss * page_size_in_bytes) / MEGABYTE;
                    pdata.processor = sdata_p->processor;
                    pdata.rt_priority = sdata_p->rt_priority;
                    pdata.policy = sdata_p->policy;
                    pdata.guest_time = sdata_p->guest_time;
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
                    snprintf(fname, FNAME_SIZE, "%s/numad.%d", CPUSET_DIR, p->pid);
                    if (access(fname, F_OK) == 0) {
                        numad_log("Removing obsolete cpuset: %s\n", fname);
                        int rc = rmdir(fname);
                        if (rc == -1) {
                            perror("bad cpuset rmdir");
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




void show_processes(process_data_p *ptr, int nprocs) {
    numad_log("Candidates: %d\n", nprocs);
    for (int ix = 0;  (ix < nprocs);  ix++) {
        process_data_p p = ptr[ix];
        char buf[BUF_SIZE];
        snprintf(buf, BUF_SIZE, "%s%s/cpuset.mems", CPUSET_DIR, p->cpuset_name);
        FILE *fs = fopen(buf, "r");
        buf[0] = '\0';
        if (fs) {
            if (fgets(buf, BUF_SIZE, fs)) {
                ELIM_NEW_LINE(buf);
            }
            fclose(fs);
        }
        numad_log("PID %d: MBs_used %ld, CPUs_used %ld, Threads %ld, Uptime %ld, Name: %s, Nodes: %s\n", 
            p->pid, p->MBs_used, p->CPUs_used, p->num_threads, p->uptime, p->comm_name, buf);
        }
    numad_log("\n");
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

    if (verbose) {
        show_processes(pindex, nprocs);
    }

    // Return maximum interval if no process movement
    return max_interval;
}




int main(int argc, char *argv[]) {
    int opt;
    while ((opt = getopt(argc, argv, "dhi:qu:vVw:")) != -1) {
	switch (opt) {
	    case 'd': debug        = 1; break;
	    case 'h': print_usage_and_exit(argv[0]); break;
	    case 'i': max_interval = atoi(optarg); break;
	    case 'q': quiet        = 1; break;
	    case 'u': target_utilization = atoi(optarg); break;
	    case 'v': verbose      = 1; break;
	    case 'V': print_version_and_exit(argv[0]); break;
            case 'w': {
                char *p = NULL;
                req_cpus = (int)strtol(optarg, &p, 10);
                if (p == optarg) {
                    printf("Can't parse req_cpus: %s\n", optarg);
                    exit(EXIT_FAILURE);
                }
                if (*p == ':') {
                    char *q = p + 1;
                    req_mbs = (int)strtol(q, &p, 10);
                    if (p == q) {
                        printf("Can't parse req_mbs: %s\n", q);
                        exit(EXIT_FAILURE);
                    }
                }
                break;
            }
	    default: print_usage_and_exit(argv[0]); break;
	}
    }
    if (argc > optind) {
	printf("Unexpected arg = %s\n", argv[optind]);
	exit(EXIT_FAILURE);
    }
    // Set verbose, if debug set
    verbose |= debug;


    check_prereqs(argv[0]);
    num_cpus = get_num_cpus();
    page_size_in_bytes = sysconf(_SC_PAGESIZE);
    huge_page_size_in_bytes = get_huge_page_size_in_bytes();


    if (!register_numad_pid()) {
        // Daemon already running
        // Send message to persistant thread to handle request
        // and exit
    }


    // daemonize self
    //
    // spawn thread to handle messages from subsequent invocation requests
    //
    // execute following loop forever

    for (;;) {
        int interval = max_interval;
        if (update_nodes() > 1) {
            update_processes();
            interval = manage_loads();
        }
        sleep(interval);
    }

    exit(EXIT_SUCCESS);
}

