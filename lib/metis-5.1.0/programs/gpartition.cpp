#include <iostream>
#include <fstream>
#include <map>
#include <set>
#include <vector>
#include <sstream>
// #include <boost/algorithm/string.hpp>

extern "C" {
#include "metisbin.h"
}

using namespace std;
// using namespace boost::algorithm;


int find_vinp(int vid, vector<set<int> > *vp)
{
    for (int i = 0; i < vp->size(); i++)
    {
        if (vp->at(i).find(vid) != vp->at(i).end())
        {
            return i;
        }
    }
    return -1;
}

int main(int argc, char *argv[])
{

    params_t *params;
    params = parse_cmdline(argc, argv);

    // string outputdir = "";

    /** input file name */
    string filename = params->filename;
    int kway = params->nparts;
    cout << "params: filename = " << filename << ", kway = " << kway << endl;

    /** recording map between original sorted id(from 1) to original vertex id */
    string map_file = filename + ".map";

    /** formated file for metis */
    string undirected_file = filename + ".udr";

    // ifstream fin(filename.c_str());

    string vfilename = filename + ".v";
    string efilename = filename + ".e";
    // ifstream fin(filename.c_str());
    ifstream vf(vfilename.c_str());
    ifstream ef(efilename.c_str());

    ofstream fmap(map_file.c_str());
    ofstream fout(undirected_file.c_str());

    map<int, set<int> > v2e;
    map<int, int> indexMap;
    int edgeCount = 0;
    int index = 1;

    string line;

    int vertex_size = 0;
    int edge_size = 0;
    while (getline(vf, line)) {
        stringstream ss(line);
        // source: source vertexID from 1;
        // vsource: origin source ID.
        int vsource, source;
        ss >> vsource;

        vertex_size++;

        if (indexMap.find(vsource) == indexMap.end())
        {
            indexMap.insert(pair<int, int>(vsource, index++));
        }
        source = indexMap.at(vsource);
        set<int> edges;
        v2e.insert(pair<int, set<int> >(source, edges));
    }
    vf.close();

    while (getline(ef, line)) {

        edge_size ++;

        stringstream ss(line);
        int from, edgetype, to;
        ss >> from >> edgetype >> to;
        if(from==to){
            cout<<"a self-loop on vertex "<<to<<endl;
            continue;
        }
        int fromID = indexMap.at(from);
        int toID = indexMap.at(to);
        v2e.find(fromID)->second.insert(toID);
        v2e.find(toID)->second.insert(fromID);
    }
    ef.close();

    cout << "scan files, v_size = " << vertex_size << ", e_size = " << edge_size << endl;
    cout << "first iteration finished." << endl;
    cout << "vertices size = " << v2e.size() << endl;
    cout << "edges size = " << edgeCount << endl;

    map<int, int> revertMap;
    for (map<int, int>::iterator it = indexMap.begin(); it != indexMap.end(); it++)
    {
        revertMap.insert(pair<int, int>(it->second, it->first));
    }

    int edgeCount2 = 0;

    for (map<int, set<int> >::iterator it = v2e.begin(); it != v2e.end(); it++)
    {
        edgeCount2 += it->second.size();
    }

    cout << edgeCount2 << endl;
    if (edgeCount2 % 2 == 1) {
        cout << "fatal:edgeCount should be even." << endl;
    }

    edgeCount2 /= 2;

    cout << "vertex and undirected edge cont = " << v2e.size() << ", " << edgeCount2 << endl;
    fout << v2e.size() << " " << edgeCount2 << endl;

    for (int i = 1; i <= revertMap.size(); i++)

    {
        fmap << i << " " << revertMap.find(i)->second << endl;

        for (set<int>::iterator it = v2e[i].begin(); it != v2e[i].end(); it++)
        {
            fout << " " << *it;
        }
        fout << endl;

    }

    fmap.close();
    fout.close();


    // now begin to invoke gpmetis.c

    idx_t i;
    char *curptr, *newptr;
    idx_t options[METIS_NOPTIONS];
    graph_t *graph;
    idx_t *part;
    idx_t objval;
    int status = 0;

    /* Change filename to formated undirected file*/
    strcpy(params->filename, undirected_file.c_str());

    gk_startcputimer(params->iotimer);
    graph = ReadGraph(params);

    ReadTPwgts(params, graph->ncon);
    gk_stopcputimer(params->iotimer);

    /* Check if the graph is contiguous */
    if (params->contig && !IsConnected(graph, 0))
    {
        printf("***The input graph is not contiguous.\n"
               "***The specified -contig option will be ignored.\n");
        params->contig = 0;
    }

    /* Get ubvec if supplied */
    if (params->ubvecstr)
    {
        params->ubvec = rmalloc(graph->ncon, "main");
        curptr = params->ubvecstr;
        for (i = 0; i < graph->ncon; i++)
        {
            params->ubvec[i] = strtoreal(curptr, &newptr);
            if (curptr == newptr)
                errexit("Error parsing entry #%"PRIDX" of ubvec [%s] (possibly missing).\n",
                        i, params->ubvecstr);
            curptr = newptr;
        }
    }

    /* Setup iptype */
    if (params->iptype == -1)
    {
        if (params->ptype == METIS_PTYPE_RB)
        {
            if (graph->ncon == 1)
                params->iptype = METIS_IPTYPE_GROW;
            else
                params->iptype = METIS_IPTYPE_RANDOM;
        }
    }

    GPPrintInfo(params, graph);

    part = imalloc(graph->nvtxs, "main: part");

    METIS_SetDefaultOptions(options);
    options[METIS_OPTION_OBJTYPE] = params->objtype;
    options[METIS_OPTION_CTYPE]   = params->ctype;
    options[METIS_OPTION_IPTYPE]  = params->iptype;
    options[METIS_OPTION_RTYPE]   = params->rtype;
    options[METIS_OPTION_NO2HOP]  = params->no2hop;
    options[METIS_OPTION_MINCONN] = params->minconn;
    options[METIS_OPTION_CONTIG]  = params->contig;
    options[METIS_OPTION_SEED]    = params->seed;
    options[METIS_OPTION_NITER]   = params->niter;
    options[METIS_OPTION_NCUTS]   = params->ncuts;
    options[METIS_OPTION_UFACTOR] = params->ufactor;
    options[METIS_OPTION_DBGLVL]  = params->dbglvl;

    gk_malloc_init();
    gk_startcputimer(params->parttimer);

    switch (params->ptype)
    {
    case METIS_PTYPE_RB:
        status = METIS_PartGraphRecursive(&graph->nvtxs, &graph->ncon, graph->xadj,
                                          graph->adjncy, graph->vwgt, graph->vsize, graph->adjwgt,
                                          &params->nparts, params->tpwgts, params->ubvec, options,
                                          &objval, part);
        break;

    case METIS_PTYPE_KWAY:
        status = METIS_PartGraphKway(&graph->nvtxs, &graph->ncon, graph->xadj,
                                     graph->adjncy, graph->vwgt, graph->vsize, graph->adjwgt,
                                     &params->nparts, params->tpwgts, params->ubvec, options,
                                     &objval, part);
        break;

    }

    gk_stopcputimer(params->parttimer);

    if (gk_GetCurMemoryUsed() != 0)
        printf("***It seems that Metis did not free all of its memory! Report this.\n");
    params->maxmemory = gk_GetMaxMemoryUsed();
    gk_malloc_cleanup(0);


    if (status != METIS_OK)
    {
        printf("\n***Metis returned with an error.\n");
    }
    else
    {
        if (!params->nooutput)
        {
            /* Write the solution */
            gk_startcputimer(params->iotimer);
            WritePartition(params->filename, part, graph->nvtxs, params->nparts);
            gk_stopcputimer(params->iotimer);
        }

        GPReportResults(params, graph, part, objval);
    }

    FreeGraph(&graph);
    gk_free((void **)&part, LTERM);

    // FIXME: due to some abort. delete the gk_free. may cause leak of memory. @Yecol
    // gk_free((void **)&params->filename, &params->tpwgtsfile, &params->tpwgts,
    //         &params->ubvecstr, &params->ubvec, &params, LTERM);



    //finished gpmetis.c
    //post-process
    //
    stringstream ss;
    ss << kway;
    string partitioned_filename = undirected_file + ".part." + ss.str();
    ifstream pfin(partitioned_filename.c_str());
    vector<set<int> > vertices_partitions;
    vector<set<string> > edges_partitions;
    vector<string> crossing_edges;
    map<int, set<int> > partition2bordernode;
    vector<int> inner_conts;

    for (int i = 0; i < kway; i++)
    {
        set<int> s;
        vertices_partitions.push_back(s);
        set<string> e;
        edges_partitions.push_back(e);
        inner_conts.push_back(0);
        set<int> bordernodes;
        partition2bordernode.insert(pair<int, set<int> >(i, bordernodes));
    }

    int p = 0;
    int lnum = 1;
    while (pfin >> p)
    {
        int vertex_id = revertMap[lnum];
        vertices_partitions[p].insert(vertex_id);
        lnum += 1;
    }

    cout << "vertices divided into partitions." << endl;


    map<int, string> vertex2labelMap;
    ifstream vfin(vfilename.c_str());
    while (getline(vfin, line)) {
        stringstream ss(line);
        int vertexID;
        string label;
        ss >> vertexID;
        getline(ss, label);
        vertex2labelMap.insert(pair<int, string>(vertexID, label));
    }
    vfin.close();


    ifstream efin(efilename.c_str());

    int crossing_cont = 0, edge_cont = 0, inner_edge_cont;

    while (getline(efin, line)) {
        int source, edgetype, target, source_p, target_p;
        stringstream ss(line);
        ss >> source >> edgetype >> target;
        edge_cont++;
        source_p = find_vinp(source, &vertices_partitions);
        target_p = find_vinp(target, &vertices_partitions);

        stringstream a_edge;
        a_edge << source << "\t" << edgetype << "\t" << target;

        if (source_p == target_p) {
            // only if the source and target in the same partition, it is a inner edge.
            edges_partitions[target_p].insert(a_edge.str());
            inner_conts[target_p]++;
            inner_edge_cont++;
        }
        else {
            crossing_cont++;
            crossing_edges.push_back(a_edge.str());
            partition2bordernode[source_p].insert(source);
            partition2bordernode[target_p].insert(target);
        }
    }

    efin.close();
    cout << "edges divided into partitions." << endl;
    cout << "total edges count = " << edge_cont << ", crossing edge = " << crossing_cont << ", ratio = " << crossing_cont * 1.0 / edge_cont << endl;

    int bordernodes = 0;
    for (int i = 0; i < kway; i++)
    {

        bordernodes += partition2bordernode[i].size();
        cout << "p" << i << " size: " << vertices_partitions[i].size() << " edges: inner = " << inner_conts[i] << ", border_nodes = " << partition2bordernode[i].size() << endl;

        stringstream ofname;
        ofname << filename << ".p" << i << ".v";
        fout.open(ofname.str().c_str());
        for (set<int>::iterator it = vertices_partitions[i].begin(); it != vertices_partitions[i].end(); it++) {
            fout << *it << "\t" << vertex2labelMap[*it] << endl;
        }
        fout.close();

        stringstream efname;
        efname << filename << ".p" << i << ".e";
        fout.open(efname.str().c_str());
        for (set<string>::iterator it = edges_partitions[i].begin(); it != edges_partitions[i].end(); it++) {
            fout << *it << endl;
        }
        fout.close();
    }

    cout << "bordernodes-size = " << bordernodes << endl;
    string of_name = filename + ".p2bv";
    fout.open(of_name.c_str());
    for (map<int, set<int> >::iterator it = partition2bordernode.begin(); it != partition2bordernode.end(); it++) {
        int partitionID = it->first;
        set<int> bnodes = it->second;
        for (set<int>::iterator it2 = bnodes.begin(); it2 != bnodes.end(); it2++) {
            fout << partitionID << "\t" << *it2 << endl;
        }

    }
    fout.close();

    string crossf = filename +".cross";
    fout.open(crossf.c_str());
    for(int i =0; i< crossing_edges.size();i++){
    	fout<<crossing_edges[i]<<endl;
    }


    fout.close();


    //remove temp files
    remove(undirected_file.c_str());
    remove(map_file.c_str());
    remove(partitioned_filename.c_str());

}





/*************************************************************************/
/*! This function prints run parameters */
/*************************************************************************/
void GPPrintInfo(params_t *params, graph_t *graph)
{
    idx_t i;

    if (params->ufactor == -1)
    {
        if (params->ptype == METIS_PTYPE_KWAY)
            params->ufactor = KMETIS_DEFAULT_UFACTOR;
        else if (graph->ncon == 1)
            params->ufactor = PMETIS_DEFAULT_UFACTOR;
        else
            params->ufactor = MCPMETIS_DEFAULT_UFACTOR;
    }

    printf("******************************************************************************\n");
    printf("%s", METISTITLE);
    printf(" (HEAD: %s, Built on: %s, %s)\n", SVNINFO, __DATE__, __TIME__);
    printf(" size of idx_t: %zubits, real_t: %zubits, idx_t *: %zubits\n",
           8 * sizeof(idx_t), 8 * sizeof(real_t), 8 * sizeof(idx_t *));
    printf("\n");
    printf("Graph Information -----------------------------------------------------------\n");
    printf(" Name: %s, #Vertices: %"PRIDX", #Edges: %"PRIDX", #Parts: %"PRIDX"\n",
           params->filename, graph->nvtxs, graph->nedges / 2, params->nparts);
    if (graph->ncon > 1)
        printf(" Balancing constraints: %"PRIDX"\n", graph->ncon);

    printf("\n");
    printf("Options ---------------------------------------------------------------------\n");
    printf(" ptype=%s, objtype=%s, ctype=%s, rtype=%s, iptype=%s\n",
           ptypenames[params->ptype], objtypenames[params->objtype], ctypenames[params->ctype],
           rtypenames[params->rtype], iptypenames[params->iptype]);

    printf(" dbglvl=%"PRIDX", ufactor=%.3f, no2hop=%s, minconn=%s, contig=%s, nooutput=%s\n",
           params->dbglvl,
           I2RUBFACTOR(params->ufactor),
           (params->no2hop   ? "YES" : "NO"),
           (params->minconn  ? "YES" : "NO"),
           (params->contig   ? "YES" : "NO"),
           (params->nooutput ? "YES" : "NO")
          );

    printf(" seed=%"PRIDX", niter=%"PRIDX", ncuts=%"PRIDX"\n",
           params->seed, params->niter, params->ncuts);

    if (params->ubvec)
    {
        printf(" ubvec=(");
        for (i = 0; i < graph->ncon; i++)
            printf("%s%.2e", (i == 0 ? "" : " "), (double)params->ubvec[i]);
        printf(")\n");
    }

    printf("\n");
    switch (params->ptype)
    {
    case METIS_PTYPE_RB:
        printf("Recursive Partitioning ------------------------------------------------------\n");
        break;
    case METIS_PTYPE_KWAY:
        printf("Direct k-way Partitioning ---------------------------------------------------\n");
        break;
    }
}


/*************************************************************************/
/*! This function does any post-partitioning reporting */
/*************************************************************************/
void GPReportResults(params_t *params, graph_t *graph, idx_t *part, idx_t objval)
{
    gk_startcputimer(params->reporttimer);
    ComputePartitionInfo(params, graph, part);

    gk_stopcputimer(params->reporttimer);

    printf("\nTiming Information ----------------------------------------------------------\n");
    printf("  I/O:          \t\t %7.3"PRREAL" sec\n", gk_getcputimer(params->iotimer));
    printf("  Partitioning: \t\t %7.3"PRREAL" sec   (METIS time)\n", gk_getcputimer(params->parttimer));
    printf("  Reporting:    \t\t %7.3"PRREAL" sec\n", gk_getcputimer(params->reporttimer));
    printf("\nMemory Information ----------------------------------------------------------\n");
    printf("  Max memory used:\t\t %7.3"PRREAL" MB\n", (real_t)(params->maxmemory / (1024.0 * 1024.0)));
    printf("******************************************************************************\n");

}
