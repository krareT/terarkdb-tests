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

struct sysinfo memInfo;

namespace benchmark {

    static unsigned long long lastTotalUser, lastTotalUserLow, lastTotalSys, lastTotalIdle;
    void firstInit(){
        FILE* file = fopen("/proc/stat", "r");
        fscanf(file, "cpu %llu %llu %llu %llu", &lastTotalUser, &lastTotalUserLow,
               &lastTotalSys, &lastTotalIdle);
        fclose(file);
    }
    double getCPUPercentage(){
        if(lastTotalUser == 0){
            firstInit();
        }

        unsigned long long totalUser, totalUserLow, totalSys, totalIdle, total;
        FILE* file;
        file = fopen("/proc/stat", "r");
        fscanf(file, "cpu %llu %llu %llu %llu", &totalUser, &totalUserLow,
               &totalSys, &totalIdle);
        fclose(file);

        double percent;
        if (totalUser < lastTotalUser || totalUserLow < lastTotalUserLow ||
            totalSys < lastTotalSys || totalIdle < lastTotalIdle){
            //Overflow detection. Just skip this value.
            percent = -1.0;
        } else{
            total = (totalUser - lastTotalUser) + (totalUserLow - lastTotalUserLow) + (totalSys - lastTotalSys);
            percent = total;
            total += (totalIdle - lastTotalIdle);
            percent /= total;
            percent *= 100;
        }

        lastTotalUser = totalUser;
        lastTotalUserLow = totalUserLow;
        lastTotalSys = totalSys;
        lastTotalIdle = totalIdle;

        return percent;
    }

    /**
     * memory usage stats
     * @return
     */
    std::vector<int> getPhysicalMemoryUsage(){
        long long totalPhysMem, physMemFree, physMemBuffer, physMemCached;
        FILE* file = fopen("/proc/meminfo", "r");
        fscanf(file, "MemTotal: %llu kB\nMemFree: %llu kB\nBuffers: %llu kB\nCached: %llu\n",
               &totalPhysMem, &physMemFree,&physMemBuffer, &physMemCached);
        fclose(file);

        std::vector<int> arr;
        // KB
        long long physMemUsed = totalPhysMem - physMemFree - physMemBuffer - physMemCached;

        // MB, total,free,cached,used
        arr.push_back(int(totalPhysMem/1024));
        arr.push_back(int(physMemFree/1024));
        arr.push_back(int((physMemBuffer + physMemCached)/1024));
        arr.push_back(int(physMemUsed/1024));
//        printf("%d %d %d %d\n", arr[0], arr[1], arr[2], arr[3]);
        return arr;
    }
}
#endif //TERARKDB_TEST_FRAMEWORK_SYSTEM_RESOURCE_H
