sequences = ["AGCTS", "GTGCC", "GGGGA"]
g_counts = []
for seq in sequences:
    for nucleotide in seq:
        g_count = 0
        if nucleotide == "G":
            g_count += 1
    g_counts.append(g_count)
print(g_counts)