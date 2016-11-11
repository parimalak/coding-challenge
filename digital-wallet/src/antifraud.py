# -*- coding: utf-8 -*-
"""
Created on Tue Nov  8 16:41:56 2016

@author:Parimala Killada
"""
import sys
import csv
import os

def read_batch(filename):
 gr = {}  
 with open(filename, 'r',encoding='utf8') as csvfile:
     if csvfile != "":
         try:
             reader = csv.reader(csvfile)
#to skip header line         
             next(reader, None)         
             for row in reader:
                 vertex1 = row[1].replace(" ","")
                 vertex2 = row[2].replace(" ","")
                 v1 = vertex1.isdigit()
                 v2 = vertex1.isdigit()
                 con = (len(row)==5 and v1 and v2)
                 while(con):
                     if vertex1 not in gr and vertex2 not in gr: 
                         gr[vertex1] = [vertex2]
                         gr[vertex2] = [vertex1]
                     elif vertex2 in gr and vertex1 not in gr:
                         gr[vertex1] = [vertex2]                  
                         if vertex1 not in gr[vertex2]:
                           gr[vertex2].append(vertex1)
                     elif vertex2 not in gr and vertex1 in gr:
                         gr[vertex2] = [vertex1]
                         if vertex2 not in gr[vertex1]:
                           gr[vertex1].append(vertex2)
                     else:
                         if vertex2 not in gr[vertex1]:
                             gr[vertex1].append(vertex2)
                         if vertex2 not in gr[vertex1]:
                             gr[vertex1].append(vertex2)    
         except IndexError: 
                pass
     else:
         sys.exit 
 return gr
 
def find_all_paths(path,start_vertex, end_vertex, graph):
        """ find all paths from start_vertex to 
            end_vertex in graph """
        sys.setrecursionlimit(10000)
        path = path + [start_vertex]
        if start_vertex == end_vertex:
            return [path]
        if start_vertex not in graph:
            return []
        paths = []
        for vertex in graph[start_vertex]:
            if vertex not in path:
                extended_paths = find_all_paths(path,vertex,end_vertex,graph)
                for p in extended_paths:   
                    paths.append(p)
        return paths
        
def minimum_path(graph, start, end, path):
    
    path = path + [start]
    if start == end:
        return path
    if start not in graph.keys():
        return []
    shortest = []
    for node in graph[start]:
        if node not in path:
            newpath = minimum_path(graph, node, end, path)
            if newpath:
                if not shortest or len(newpath) < len(shortest):
                    shortest = newpath
    return shortest 
       
def output_files(paths,output_file1,output_file2,output_file3):
    out1 = output_file1
    out2 = output_file2
    out3 = output_file3
    f1 = open(out1,"w")
    f2 = open(out2,"w")
    f3 = open(out3,"w")
 
    paths.sort()
    if paths != []:
     min = len(paths[0])
     max = len(paths[-1])
    else:
     min = 0
     max = 0

    if min == 1:
        f1.write("trusted"+os.linesep)
        f2.write("trusted"+os.linesep)
        f3.write("trusted"+os.linesep)
    elif min ==2 :
        f1.write("unverified"+os.linesep)
        f2.write("trusted"+os.linesep)
        f3.write("trusted"+os.linesep) 
    elif min >= 3 and max <=4 :
        f1.write("unverified"+os.linesep)
        f2.write("unverified"+os.linesep)
        f3.write("trusted"+os.linesep)
    else:
        f1.write("unverified"+os.linesep)
        f2.write("unverified"+os.linesep)
        f3.write("unverified"+os.linesep)

    f1.close()
    f2.close()
    f3.close()
    return 
    
def stream_process(graph,filename,output_file1,output_file2,output_file3):
 graph = graph
 path =[]
 paths =[]
 with open(filename, 'r',encoding='utf8') as csvfile:
     if csvfile != "":
         try:
             reader = csv.reader(csvfile)
#to skip header line         
             next(reader, None)         
             for row in reader:
                 if len(row)<5:
                     next(reader,None)
                 else:
                     vertex1 = row[1]
                     vertex2 = row[2]
                     paths=minimum_path(graph, vertex1, vertex2, path)
                    # paths = find_all_paths(path,vertex1, vertex2,graph)
                     output_files(paths,output_file1,output_file2,output_file3)
         except Exception as e: 
                print(e)
     else:
         sys.exit 
 return 
 
def format():
   print("Usage python3 ./src/antifraud.py ./paymo_input/batch_payment.txt ./paymo_input/stream_payment.txt ./paymo_output/output1.txt ./paymo_output/output2.txt ./paymo_output/output3.txt")

def main(argv):
    if len(argv) < 5:
        format()
        sys.exit(2)    
    batch_file = argv[0]
    stream_fname =argv[1]
    output_file1 = argv[2]
    output_file2 = argv[3]
    output_file3 = argv[4]     
    g = read_batch(batch_file)
    stream_process(g,stream_fname,output_file1,output_file2,output_file3)
    #output_files(paths1,output_file1,output_file2,output_file3)

if __name__ == "__main__":
    print(sys.argv)
    main(sys.argv[1:]) 
    