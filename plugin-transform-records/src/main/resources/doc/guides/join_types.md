# Join types guide

## Inner Join
```mermaid
---
config:
  venn:
    width: 400
    height: 225
  themeCSS: ".venn-text-node { font-size: 40px; transform: translateY(-7.5px); } .venn-text-area:nth-child(1) .venn-text-node { transform: translate(-7px, -7.5px); } .venn-text-area:nth-child(2) .venn-text-node { transform: translate(7px, -7.5px); } .venn-text-area:nth-child(3) .venn-text-node { font-size: 20px; }"
---
venn-beta
    set A:10
    set B:10
        text labelB["B"]
    set A2:10
    union A,A2:10
        text labelA["A"]
    union A,B:3
    union A2,B:3
    union A,A2,B:3
        text innerFields["A + B fields"]
    style A fill:#ffffff,fill-opacity:1,stroke:#000000,color:transparent
    style B fill:#ffffff,fill-opacity:1,stroke:#000000,color:transparent
    style A2 fill:#ffffff,fill-opacity:0,stroke:#000000,color:transparent
    style A,A2,B fill:#ccff00
    style labelA color:#000000
    style labelB color:#000000
    style innerFields color:#000000
```
### Matching rows between A and B.
#### SQL equivalent: 
```sql
SELECT *
FROM A
JOIN B ON A.column = B.column
```
---
## Full Outer Join
```mermaid
---
config:
  venn:
    width: 400
    height: 225
  themeCSS: ".venn-text-node { font-size: 40px; transform: translateY(-7.5px); } .venn-text-area:nth-child(1) .venn-text-node { transform: translate(-7px, -7.5px); } .venn-text-area:nth-child(2) .venn-text-node { transform: translate(7px, -7.5px); } .venn-text-area:nth-child(3) .venn-text-node { font-size: 20px; }"
---
venn-beta
    set A:10
    set B:10
        text labelB["B"]
    set A2:10
    union A,A2:10
        text labelA["A"]
    union A,B:3
    union A2,B:3
    union A,A2,B:3
    style A fill:#ccff00,fill-opacity:1,stroke:#000000,color:transparent
    style B fill:#ccff00,fill-opacity:1,stroke:#000000,color:transparent
    style A2 fill:#ffffff,fill-opacity:0,stroke:#000000,color:transparent
    style A,A2,B fill:#ccff00
    style labelA color:#000000
    style labelB color:#000000
```
### Rows from A and B, with matching or not.
#### SQL equivalent: 
```sql
SELECT *
FROM A
FULL JOIN B ON A.column = B.column
```
---
## Left Outer Join
```mermaid
---
config:
  venn:
    width: 400
    height: 225
  themeCSS: ".venn-text-node { font-size: 40px; transform: translateY(-7.5px); } .venn-text-area:nth-child(1) .venn-text-node { transform: translate(-7px, -7.5px); } .venn-text-area:nth-child(2) .venn-text-node { transform: translate(7px, -7.5px); } .venn-text-area:nth-child(3) .venn-text-node { font-size: 20px; }"
---
venn-beta
    set A:10
    set B:10
        text labelB["B"]
    set A2:10
    union A,A2:10
        text labelA["A"]
    union A,B:3
    union A2,B:3
    union A,A2,B:3
    style A fill:#ccff00,fill-opacity:1,stroke:#000000,color:transparent
    style B fill:#ffffff,fill-opacity:1,stroke:#000000,color:transparent
    style A2 fill:#ffffff,fill-opacity:0,stroke:#000000,color:transparent
    style A,A2,B fill:#ccff00
    style labelA color:#000000
    style labelB color:#000000
```
### Rows from A, matching with B or not.
#### SQL equivalent: 
```sql
SELECT *
FROM A
LEFT JOIN B ON A.column = B.column
```
---
## Right Outer Join
```mermaid
---
config:
  venn:
    width: 400
    height: 225
  themeCSS: ".venn-text-node { font-size: 40px; transform: translateY(-7.5px); } .venn-text-area:nth-child(1) .venn-text-node { transform: translate(-7px, -7.5px); } .venn-text-area:nth-child(2) .venn-text-node { transform: translate(7px, -7.5px); } .venn-text-area:nth-child(3) .venn-text-node { font-size: 20px; }"
---
venn-beta
    set A:10
    set B:10
        text labelB["B"]
    set A2:10
    union A,A2:10
        text labelA["A"]
    union A,B:3
    union A2,B:3
    union A,A2,B:3
    style A fill:#ffffff,fill-opacity:1,stroke:#000000,color:transparent
    style B fill:#ccff00,fill-opacity:1,stroke:#000000,color:transparent
    style A2 fill:#ffffff,fill-opacity:0,stroke:#000000,color:transparent
    style A,A2,B fill:#ccff00
    style labelA color:#000000
    style labelB color:#000000
```
### Rows from B, matching with A or not.
#### SQL equivalent: 
```sql
SELECT *
FROM A
RIGHT JOIN B ON A.column = B.column
```
---
## Left Anti Join
```mermaid
---
config:
  venn:
    width: 400
    height: 225
  themeCSS: ".venn-text-node { font-size: 40px; transform: translateY(-7.5px); } .venn-text-area:nth-child(1) .venn-text-node { transform: translate(-7px, -7.5px); } .venn-text-area:nth-child(2) .venn-text-node { transform: translate(7px, -7.5px); } .venn-text-area:nth-child(3) .venn-text-node { font-size: 20px; }"
---
venn-beta
    set A:10
    set B:10
        text labelB["B"]
    set A2:10
    union A,A2:10
        text labelA["A"]
    union A,B:3
    union A2,B:3
    union A,A2,B:3
    style A fill:#ccff00,fill-opacity:1,stroke:#000000,color:transparent
    style B fill:#ffffff,fill-opacity:1,stroke:#000000,color:transparent
    style A2 fill:#ffffff,fill-opacity:0,stroke:#000000,color:transparent
    style A,A2,B fill:#ffffff
    style labelA color:#000000
    style labelB color:#000000
```
### Rows from A with no match with B.
#### SQL equivalent:
```sql
SELECT *
FROM A
LEFT JOIN B ON A.column = B.column
WHERE B.column IS NULL
```
---
## Right Anti Join
```mermaid
---
config:
  venn:
    width: 400
    height: 225
  themeCSS: ".venn-text-node { font-size: 40px; transform: translateY(-7.5px); } .venn-text-area:nth-child(1) .venn-text-node { transform: translate(-7px, -7.5px); } .venn-text-area:nth-child(2) .venn-text-node { transform: translate(7px, -7.5px); } .venn-text-area:nth-child(3) .venn-text-node { font-size: 20px; }"
---
venn-beta
    set A:10
    set B:10
        text labelB["B"]
    set A2:10
    union A,A2:10
        text labelA["A"]
    union A,B:3
    union A2,B:3
    union A,A2,B:3
    style A fill:#ffffff,fill-opacity:1,stroke:#000000,color:transparent
    style B fill:#ccff00,fill-opacity:1,stroke:#000000,color:transparent
    style A2 fill:#ffffff,fill-opacity:0,stroke:#000000,color:transparent
    style A,A2,B fill:#ffffff
    style labelA color:#000000
    style labelB color:#000000
```
### Rows from B with no match with A.
#### SQL equivalent:
```sql
SELECT *
FROM A
RIGHT JOIN B ON A.column = B.column
WHERE A.column IS NULL
```
---
## Left Semi Join
```mermaid
---
config:
  venn:
    width: 400
    height: 225
  themeCSS: ".venn-text-node { font-size: 40px; transform: translateY(-7.5px); } .venn-text-area:nth-child(1) .venn-text-node { transform: translate(-7px, -7.5px); } .venn-text-area:nth-child(2) .venn-text-node { transform: translate(7px, -7.5px); } .venn-text-area:nth-child(3) .venn-text-node { font-size: 20px; }"
---
venn-beta
    set A:10
    set B:10
        text labelB["B"]
    set A2:10
    union A,A2:10
        text labelA["A"]
    union A,B:3
    union A2,B:3
    union A,A2,B:3
        text leftSemiFields["A fields only"]
    style A fill:#ffffff,fill-opacity:1,stroke:#000000,color:transparent
    style B fill:#ffffff,fill-opacity:1,stroke:#000000,color:transparent
    style A2 fill:#ffffff,fill-opacity:0,stroke:#000000,color:transparent
    style A,A2,B fill:#ccff00
    style labelA color:#000000
    style labelB color:#000000
    style leftSemiFields color:#000000
```
### Rows from A matching with B.
#### Similar to Inner Join but without containing data from B.
#### SQL equivalent:
```sql
SELECT *
FROM A
WHERE column IN (
    SELECT column
    FROM B
)
```
---
## Right Semi Join
```mermaid
---
config:
  venn:
    width: 400
    height: 225
  themeCSS: ".venn-text-node { font-size: 40px; transform: translateY(-7.5px); } .venn-text-area:nth-child(1) .venn-text-node { transform: translate(-7px, -7.5px); } .venn-text-area:nth-child(2) .venn-text-node { transform: translate(7px, -7.5px); } .venn-text-area:nth-child(3) .venn-text-node { font-size: 20px; }"
---
venn-beta
    set A:10
    set B:10
        text labelB["B"]
    set A2:10
    union A,A2:10
        text labelA["A"]
    union A,B:3
    union A2,B:3
    union A,A2,B:3
        text rightSemiFields["B fields only"]
    style A fill:#ffffff,fill-opacity:1,stroke:#000000,color:transparent
    style B fill:#ffffff,fill-opacity:1,stroke:#000000,color:transparent
    style A2 fill:#ffffff,fill-opacity:0,stroke:#000000,color:transparent
    style A,A2,B fill:#ccff00
    style labelA color:#000000
    style labelB color:#000000
    style rightSemiFields color:#000000
```
### Rows from B matching with A.
#### Similar to Inner Join but without containing data from A.
#### SQL equivalent:
```sql
SELECT *
FROM B
WHERE column IN (
    SELECT column
    FROM A
)
```
