# 165a-winter-2026

# L-Store Milestone 2

Milestone 2 extends M1 by adding:

- PageRange + PageDirectory
- Secondary Index
- Background Merge (Data Reorganization)
- Bufferpool + Durability (open/close)


## Record Model

Each logical record consists of:
--- One Base Record
--- Zero or more Tail Records (updates)

Updates always create new tail records.  
Versions form a chain:

Base → Tail (latest) → Tail (older) → ...


## Physical Storage

Data is stored in columnar format using fixed-size pages.

--- Page size: 4096 bytes  
--- Value size: 8 bytes (int)  
--- Capacity: 512 values per page  
--- One page stores one column only


## Column Layout

Each record is stored as:

| Index | Meaning |
|------|--------|
| 0 | Indirection |
| 1 | RID |
| 2 | Timestamp |
| 3 | Schema Encoding |
| 4+ | User Columns |


## RID Rules

- Base RIDs start from 1
- Tail RIDs start from 10,000,000
- RIDs never change
- Use:
  - `alloc_base_rid()`
  - `alloc_tail_rid()`


## Indirection

- Base.indirection → latest tail RID  
- Tail.indirection → previous version RID  
- If never updated: base.indirection = 0  


### Base Record

Points to the latest tail record.

base.indirection = latest_tail_rid  

If the record has never been updated:

base.indirection = 0


### Tail Record

Points to the previous version:

tail.indirection = previous_rid  

This forms a linked version chain:

Base → Tail3 → Tail2 → Tail1


## Index

- Can build index on any column
- Query uses index if available
- Otherwise fallback to scan
- Must maintain index on insert/update/delete


## Important
--- Never touch Page or PageRange directly.  
--- Always use write_record() and read_record().