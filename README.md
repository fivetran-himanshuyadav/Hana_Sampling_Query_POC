# Comparison

|  Query | Execution Time | Median | SD | Min - Max size of Bucket | Expected Bucket size |
| --- | --- | --- | --- | --- | --- |
| **Bernoulli** | ≤ 1 sec | 71452.0 | 107084.89 | 41 - 676740 | 100K |
| **System** | ≤ 1 sec | 1241.0 | 226777.97 | 2 - 4876528 | 100K |
| **Row-Wise-Predictate** | ≤ 17 sec, CPU - 500+ |  |  | 11 -  | 100K |
| **RowNumber** | NC, CPU - 800+ |  |  |   | 100K |
| **rowId modulo** | 22sec, CPU - 800+ |  |  |   | 100K |



NC: Not Completed

# Queries
### Bernoulli
```
SELECT DOKVERSION, ID, LANGU, LINE, OBJECT, TYP, 'm' AS dummy_column
FROM SAPABAP1.DOKTL
TABLESAMPLE BERNOULLI(0.001)
ORDER BY DOKVERSION, ID, LANGU, LINE, OBJECT, TYP
```

### System Sampling
```
SELECT DOKVERSION, ID, LANGU, OBJECT, LINE, TYP, 'm' AS dummy_column
                FROM SAPABAP1.DOKTL
                TABLESAMPLE SYSTEM(0.001)
                ORDER BY OBJECT, LINE, DOKVERSION, ID, LANGU, TYP
```

### Row Wise Predictate
```
SELECT DOKVERSION, ID, LANGU, OBJECT, LINE, TYP, 'm' AS dummy_column
                FROM SAPABAP1.DOKTL
                WHERE
                    RAND() < 0.001
                ORDER BY OBJECT, LINE, DOKVERSION, ID, LANGU, TYP
```

### Using RowNumber
```
SELECT DOKVERSION, ID, LANGU, OBJECT, LINE, TYP
FROM (
  SELECT DOKVERSION, ID, LANGU, OBJECT, LINE, TYP,
         ROW_NUMBER() OVER (ORDER BY DOKVERSION, ID, LANGU, OBJECT, LINE, TYP) AS rn
  FROM SAPABAP1.DOKTL
) ranked
WHERE MOD(rn, 100000) = 1;
```

### Modulo on row id
```
SELECT "$rowid$", DOKVERSION, ID, LANGU, OBJECT, LINE, TYP
FROM SAPABAP1.DOKTL
WHERE MOD("$rowid$", 100000) = 1;
```

