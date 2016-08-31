//
// Created by terark on 16-8-15.
//

#ifndef TERARKDB_TEST_FRAMEWORK_SYSTEM_RESOURCE_H
#define TERARKDB_TEST_FRAMEWORK_SYSTEM_RESOURCE_H

#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "sys/types.h"
#include "sys/sysinfo.h"
#include "sys/stat.h"
#include "ftw.h"

struct sysinfo memInfo;

namespace benchmark {

    static unsigned long long lastTotalUser, lastTotalUserLow, lastTotalSys, lastTotalIdle, lastTotalIOWait;

    void firstInit() {
        FILE *file = fopen("/proc/stat", "r");
        fscanf(file, "cpu %llu %llu %llu %llu %llu", &lastTotalUser, &lastTotalUserLow,
               &lastTotalSys, &lastTotalIdle, &lastTotalIOWait);
        fclose(file);
    }

    void getCPUPercentage(double *const cpu) {
        if (lastTotalUser == 0) {
            firstInit();
        }

        unsigned long long totalUser, totalUserLow, totalSys, totalIdle, totalIOWait, total_used, total_all;
        FILE *file;
        file = fopen("/proc/stat", "r");
        int count = fscanf(file, "cpu %llu %llu %llu %llu %llu", &totalUser, &totalUserLow,
                           &totalSys, &totalIdle, &totalIOWait);
        fclose(file);

        if (count < 5) {
            return;
        }

        double usage, iowait;
        if (totalUser < lastTotalUser || totalUserLow < lastTotalUserLow ||
            totalSys < lastTotalSys || totalIdle < lastTotalIdle || totalIOWait < lastTotalIOWait) {
            //Overflow detection. Just skip this value.
            usage = -1.0;
            iowait = -1.0;
        } else {
            // cpu usage
            total_used = (totalUser - lastTotalUser) + (totalUserLow - lastTotalUserLow) + (totalSys - lastTotalSys);
            total_all = total_used + totalIdle - lastTotalIdle + totalIOWait - lastTotalIOWait;
            usage = double(total_used) / total_all;
            usage *= 100;
            // cpu iowait
            iowait = double(totalIOWait - lastTotalIOWait) / total_all;
            iowait *= 100;

        }
        cpu[0] = usage;
        cpu[1] = iowait;

        lastTotalUser = totalUser;
        lastTotalUserLow = totalUserLow;
        lastTotalSys = totalSys;
        lastTotalIdle = totalIdle;
        lastTotalIOWait = totalIOWait;
    }

    /**
     * memory usage stats
     * @return
     */
    void getPhysicalMemoryUsage(std::vector<int> &arr) {
        int totalPhysMem, physMemFree, physMemAvaliable, physMemBuffer, physMemCached;
        FILE *file = fopen("/proc/meminfo", "r");
        fscanf(file, "MemTotal: %d kB MemFree: %d kB MemAvailable: %d kB Buffers: %d kB Cached: %d kB",
               &totalPhysMem, &physMemFree, &physMemAvaliable, &physMemBuffer, &physMemCached);
        fclose(file);

//        printf("%d %d %d %d %d\n", totalPhysMem, physMemAvaliable, physMemFree, physMemBuffer, physMemCached);

        // KB
        int physMemUsed = totalPhysMem - physMemFree - physMemBuffer - physMemCached;

        // MB, total,free,cached,used
        arr.push_back(totalPhysMem / 1024);
        arr.push_back(physMemFree / 1024);
        arr.push_back((physMemBuffer + physMemCached) / 1024);
        arr.push_back(physMemUsed / 1024);
    }


    int getDiskUsageByKB(const char *path) {
        int result = 0;
        char buffer[256];

        snprintf(buffer, sizeof(buffer), "du -sk %s", path);
        FILE *fp = popen(buffer, "r");
        if (fp != NULL && fgets(buffer, 256, fp)) {
            sscanf(buffer, "%d", &result);
        }
        if (pclose(fp)) {
            printf("exit popen(`du -sk`) with errors\n");
        }
        return result;
    }
}
#endif //TERARKDB_TEST_FRAMEWORK_SYSTEM_RESOURCE_H
