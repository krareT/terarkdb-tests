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
    void getPhysicalMemoryUsage(int arr[4]) {
        int totalPhysMem, physMemFree, physMemAvaliable, physMemBuffer, physMemCached;
        FILE *file = fopen("/proc/meminfo", "r");
        fscanf(file, "MemTotal: %d kB MemFree: %d kB MemAvailable: %d kB Buffers: %d kB Cached: %d kB",
               &totalPhysMem, &physMemFree, &physMemAvaliable, &physMemBuffer, &physMemCached);
        fclose(file);

//        printf("%d %d %d %d %d\n", totalPhysMem, physMemAvaliable, physMemFree, physMemBuffer, physMemCached);

        // KB
        int physMemUsed = totalPhysMem - physMemFree - physMemBuffer - physMemCached;

        // MB, total,free,cached,used
        arr[0] = totalPhysMem / 1024;
        arr[1] = physMemFree / 1024;
        arr[2] = (physMemBuffer + physMemCached) / 1024;
        arr[3] = physMemUsed / 1024;
    }

    long long getDiskUsageByKB(const std::vector<std::string>& pathes) {
        std::string cmd("du -sk");
        for (const auto& path : pathes) {
            cmd += " ";
            cmd += path;
        }
        FILE *fp = popen(cmd.c_str(), "r");
        if (!fp) {
            fprintf(stderr, "popen(%s) = %s\n", cmd.c_str(), strerror(errno));
            return 0;
        }
        long long result = 0;
        char buffer[256];
        while (fgets(buffer, sizeof(buffer), fp)) {
            char* endptr = NULL;
            long long oneDirSize = strtoll(buffer, &endptr, 10);
            result += oneDirSize;
        }
        int ret = pclose(fp);
        if (ret != 0) {
            fprintf(stderr, "getDiskUsageByKB = %lld: pclose(%s) = %d, err = %s\n"
                    , result, cmd.c_str(), ret, strerror(errno));
        }
        return result;
    }

    void getDiskFileInfo(const std::vector<std::string>& pathes, std::string& info) {
        char buffer[256];
        std::string cmd("du -h");
        for (const auto& path : pathes) {
            cmd += " ";
            cmd += path;
        }
        cmd += " | sort -k2";
        FILE *fp = popen(cmd.c_str(), "r");
        if (!fp) {
            fprintf(stderr, "popen(%s) = %s\n", cmd.c_str(), strerror(errno));
            return;
        }
        while(fgets(buffer, sizeof(buffer), fp)) {
            info += buffer;
        }
        if(pclose(fp)){
            fprintf(stderr, "getDiskFileInfo: pclose(%s) = %s\n", cmd.c_str(), strerror(errno));
        }
    }
}
#endif //TERARKDB_TEST_FRAMEWORK_SYSTEM_RESOURCE_H
