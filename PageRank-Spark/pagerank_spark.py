from pathlib import Path

from pyspark import SparkContext
import sys
import re
import subprocess
import time



def parser(line):
    begin_title = line.find("<title>") + len("<title>")
    end_title = line.find("</title>")
    # get the title
    if begin_title < end_title:
        page_title = line[begin_title:end_title]
    else:
        print("Title not found")
        sys.exit(-1)

    begin_text = line.find("<text") + len("<text")
    end_text = line.find("</text>")
    # get the title of the outlink
    if begin_text < end_text:
        page_text = line[begin_text:end_text]
        outlinks = re.findall(r'\[\[([^]]*)\]\]', page_text)
    else:
        print("Text not found")
        sys.exit(-1)
    return page_title, outlinks


def compute_contribution(title, outlinks, rank):

    rank_list = [(title, 0)]
    noutlinks = len(outlinks)
    contribution = rank/noutlinks

    for link in outlinks:
        rank_list.append((link, contribution))

    return rank_list


def compute_rank(rank):
    return float(alfa / N) + (1 - alfa) * float(rank)




if __name__ == '__main__':

    if len(sys.argv) != 4:
        # args: [1]=input_file(txt),   [2]=alpha    [3]=nIterations
        print("Usage: pagerank_spark.py <input_file> <alpha> <number of iterations>", file=sys.stderr)
        sys.exit(-1)
    inputFile = sys.argv[1]
    alfa = float(sys.argv[2])
    iterations = int(sys.argv[3])

    ts_start = time.time()

    master = "yarn"
    sc = SparkContext(master, "PageRank")
    inputRDD = sc.textFile(inputFile)

    # compute N and broadcast it to the workers
    N = inputRDD.count()
    

    #create <title, outlinks> rdds
    graph = inputRDD.map(lambda line: parser(line))

    # remove nodes without outlinks
    graph = graph.filter(lambda node: len(node[1]) >= 1).cache()

    # create the list of titles used later to partially discriminate those having outlinks from the others
    pagetitle_keys = graph.keys().collect()
    broadcastK = sc.broadcast(pagetitle_keys)

    # set the initial pagerank (1/N)
    page_ranks = graph.mapValues(lambda value: 1 / N)

    for i in range(iterations):
        complete_graph = graph.join(page_ranks)
        titles_contributions = complete_graph.flatMap(lambda node_tuple: compute_contribution(node_tuple[0], node_tuple[1][0], node_tuple[1][1]))
        #0 -> title
        #1 -> outlinks
        #2 -> rank
        # take the only contributions that are relative to considered nodes
        titles_contributions_filtered = titles_contributions.filter(lambda x: x[0] in broadcastK.value)
       
        page_ranks = titles_contributions_filtered.reduceByKey(lambda x, y: x+y).mapValues(lambda rank: compute_rank(rank))

    # sort by value (pagerank)
    sorted_pageranks = page_ranks.sortBy(lambda page: page[1], False)

    #remove the old output. If present will show the CLI message: 'rm: `sparkoutput': No such file or directory'.
    subprocess.call(["hadoop", "fs", "-rm", "-r", "sparkoutput"])
    sorted_pageranks.saveAsTextFile("sparkoutput")
    ts_end = time.time()
    print("Execution time: ", ts_end-ts_start)
