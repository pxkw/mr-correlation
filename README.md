# About

For studying Map Reduce by calculating Pearson's correlation.

# Architecture

- Job1: Calculating errors from average for each item.
- Job2: Calculating Peasson's correlation using errors from Job1.

## Job1: Calculating errors 

**Mapper**

In -> Out
```
"Shop,ItemA,ItemB,ItemC" -> (no output)
"Shop01,12,28,38" -> {"ItemA,ItemB,ItemC":"12,28,38"}
"Shop02,38,35,48" -> {"ItemA,ItemB,ItemC":"38,35,48"}
"Shop03,28,55,24" -> ...
```

**Reducer**

In
```
{"ItemA,ItemB,ItemC":"12,28,38"}
{"ItemA,ItemB,ItemC":"38,35,48"}
...
```

Out
```
ItemA,ItemB,ItemC 28.8,31.6,3.8
ItemA,ItemB,ItemC 3.8,24.6,-6.2
...
```


## Job2: Calculating Peasson's correlation

**Mapper**

In1 -> Out1
```
"ItemA,ItemB,ItemC 28.8,31.6,3.8" -> {"ItemA,ItemB#a": "28.8,31.6"} {"ItemA,ItemC":"28.8,3.8"}, {"ItemB,ItemC":"31.6,3.8"}
"ItemA,ItemB,ItemC 3.8,24.6,-6.2" -> {"ItemA,ItemB#a": "3.8,24.6"} {"ItemA,ItemC":"3.8,-6.2"}, {"ItemB,ItemC":"24.6,-6.2"}
...
```

In2 -> Out2
```
"Shop,ItemA,ItemB,ItemC" -> (no output)
"Shop01,12,28,38" -> {"ItemA,ItemB":"12,28"} {"ItemA,ItemC":"12,38"},{"ItemB,ItemC":"28,38"}
...
```

**Partitioner, Comparator and Sorter config**

Let a key `ItemA,ItemB#a` to be treated with `ItemA,ItemB`s and put it on the first of stream.


**Reducer**

In
```
{"ItemA,ItemB#a":"40.8,59.6"},
{"ItemA,ItemB":"12,28"},
{"ItemA,ItemB":"38,35"}

```

Out
```
ItemA,ItemB,0.8654509489
```


# Reference

- https://blog.apar.jp/data-analysis/2822/

# Data

Correct correlations are calculated by LibreOffice Calc like:

```
CORREL=(A2:A6,B2,B6)
```
