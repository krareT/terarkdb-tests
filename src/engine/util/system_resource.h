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

static unsigned long long lastTotalUser, lastTotalUserLow, lastTotalSys, lastTotalIdle;

namespace benchmark {

    double getCPUPercentage(){
        double percent;
        FILE* file;
        unsigned long long totalUser, totalUserLow, totalSys, totalIdle, total;

        file = fopen("/proc/stat", "r");
        fscanf(file, "cpu %llu %llu %llu %llu", &totalUser, &totalUserLow,
               &totalSys, &totalIdle);
        fclose(file);

        if (totalUser < lastTotalUser || totalUserLow < lastTotalUserLow ||
            totalSys < lastTotalSys || totalIdle < lastTotalIdle){
            //Overflow detection. Just skip this value.
            percent = -1.0;
        }
        else{
            total = (totalUser - lastTotalUser) + (totalUserLow - lastTotalUserLow) +
                    (totalSys - lastTotalSys);
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

    std::vector<int> getPhysicalMemoryUsage(){
        sysinfo (&memInfo);
        std::vector<int> arr;
        // KB
        int totalPhysMem = memInfo.totalram;
        int physMemFree = memInfo.freeram;
        int physMemBuffer = memInfo.bufferram;
        int physMemUsed = memInfo.totalram - memInfo.freeram;

        // MB
        arr.push_back(totalPhysMem/1024);
        arr.push_back(physMemFree/1024);
        arr.push_back(physMemBuffer/1024);
        arr.push_back(physMemUsed/1024);
        return arr;
    }

}
#endif //TERARKDB_TEST_FRAMEWORK_SYSTEM_RESOURCE_H
