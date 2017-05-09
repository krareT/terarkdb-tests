#!/usr/bin/env python3

import sys
import os
from numpy import *
from matplotlib import pyplot as plt
from matplotlib import cm, colors
from matplotlib import ticker
import json

def gen_frame(path):
    jsonObj = {
        "figsize": [12, 9],
        "title": "", 

        "xlabel": "",
        "xdata": [],

        "ylablel": "",
        "ydata": []
    }
    root, ext = os.path.splitext(path)
    path = root + ".json"
    with open(path, "w") as f:
        json.dump(jsonObj, f, indent = '\t')
        print("generated the frame in", path)

def draw(path):
    root, ext = os.path.splitext(path)
    path = root + ".json"
    with open(path, "r") as f:
        # file name
        root, ext = os.path.splitext(path)
        svgname = root + ".svg"
        pngname = root + ".png"

        jsonObj = json.load(f)

        # assignment
        n = len(jsonObj["ydata"])
        width, height = jsonObj["figsize"][0], jsonObj["figsize"][1]
        barwidth = 0.8 if n > 1 else 0.4
        fontsize = height * 2.0
        title = jsonObj["title"]
        x = (jsonObj["xlabel"], jsonObj["xdata"])
        ys = [(obj["label"], obj["ydata"]) for obj in jsonObj["ydata"]]
        # print(ys)

        # plot
        fig, ax = plt.subplots(figsize = (width, height))

        # show and set grid and hide the frame
        ax.grid(True)
        for line in ax.get_ygridlines():
            line.set_linestyle(':')
        ax.spines['top'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['right'].set_visible(False)
        for line in ax.get_xgridlines():
            line.set_visible(False)
        
        ind = arange(0, len(x[1]) * n, n) # xticks indices
        for i in range(n): # plot bars
            rects = ax.bar(ind + (i - (n - 1) / 2.0) * barwidth, ys[i][1], width = barwidth, label = ys[i][0])
            for rect, y in zip(rects, ys[i][1]): # show value above the bar
                ax.text(rect.get_x() + rect.get_width()/2., rect.get_height()+0.1,
                            # '%f' % float(ys[i][1]),
                            '{:,}'.format(y),
                            fontsize = fontsize, 
                            ha='center', va='bottom')
        if n > 1: # for more than one series of data, need legends
            ax.set_position([0.1, 0.1, 0.7, 0.8])
            plt.legend(bbox_to_anchor=(1.01, 0.5), loc=2, fontsize = fontsize)

        # some axis format settings
        ax.set_title(title, fontdict = {'weight': 'bold', 'size': fontsize * 1.5}, y= 1.02)
        ax.set_xlabel(x[0], fontdict = {'weight': 'normal', 'size': fontsize})
        ax.set_xticks(ind)
        ax.set_xticklabels(x[1], fontsize = fontsize)
        ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
        for yticklabel in ax.get_yticklabels():
            yticklabel.set_fontsize(fontsize)
        # save
        plt.savefig(svgname)
        plt.savefig(pngname)
        # plt.show()
        print('save figure to', svgname, pngname)

if __name__ == "__main__":
    if len(sys.argv) != 3: 
        print("usage:", sys.argv[0], "init/plot", "/path/to/data.txt")
        print("init: generate a new empty json file (/path/to/data.json) with necessary attributes")
        print("plot: draw charts according to /path/to/data.txt")
        exit(1)
    
    if sys.argv[1] == "init":
        gen_frame(sys.argv[2])
    elif sys.argv[1] == "plot":
        draw(sys.argv[2])
    else:
        print("usage:", sys.argv[0], "init/plot", "/path/to/data.txt")
        print("init: generate a new empty json file (/path/to/data.json) with necessary attributes")
        print("plot: draw charts according to /path/to/data.txt")
