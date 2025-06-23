# Comparison


| **Query** | **Execution Time** | **Median** | **SD** | **Min - Max size of Bucket** | **Expected Bucket size** | **Time Execution** | **Distribution** |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **Bernoulli** | ≤ 1 sec | 71452.0 | 107084.89 | 41 - 676740 | 100K | ✅ Fast | ❌ Bad  |
| **System** | ≤ 1 sec | 1241.0 | 226777.97 | 2 - 4876528 | 100K | ✅ Fast | ❌ Bad  |
| **Row-Wise-Predictate** | ≤ 17 sec, CPU - 500+ |  |  | 11 - | 100K | ❌ Slow | ❌ Bad  |
| **RowNumber** | NC, CPU - 800+ |  |  |  | 100K | ❌ Very very slow | ✅ Deterministic |
| **rowId modulo** | 22sec, CPU - 800+ | 74333.0 | 94134.99 | 8 - 589641  | 100K | ❌ slow | ❌ Bad  |


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
SELECT * FROM (
SELECT DOKVERSION, ID, LANGU, OBJECT, LINE, TYP
FROM SAPABAP1.DOKTL
WHERE MOD("$rowid$", 100000) = 1
) ORDER BY DOKVERSION, ID, LANGU, OBJECT, LINE, TYP

```

