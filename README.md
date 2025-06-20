### Performance

    1. Time Execution: 1 sec
    2. Median: 71452.0
    3. Standard Deviation: 107084.89 (very large)
    4. Histogram: Left skewed


    < 10K : 82
	[10k 20k]: 55
	[20k - 30k]: 67
	[30K - 40k]: 41
	[40K - 50k]: 46
	[50 - 60]: 49
	[60 - 70]: 42
	[70 - 80]: 43
	[80 - 90]: 27
	[90 - 100]: 28

### Conclusion
1. Query execution time is under limit
2. But data is not distributed well among buckets.
3. Most buckets have very less data than expected.