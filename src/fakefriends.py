import igraph
g = igraph.Graph.Barabasi(1000000, 15, directed=False)
print "Edges: " + str(g.ecount()) + " Verticies: " + str(g.vcount())
g.write_edgelist("fakefriends.txt")


