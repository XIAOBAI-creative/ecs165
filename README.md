# 165a-winter-2026

# L-Store Milestone 1

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

--- Base RIDs start from 1
--- Tail RIDs start from 10,000,000
--- RIDs are unique and never change  
--- generate using table.alloc_base_rid() and table.alloc_tail_rid()

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

## Indirection

--- Base record indirection → latest tail RID  
--- Tail record indirection → previous version RID  
--- If no update: base.indirection = 0  

## Important
--- Never touch Page or PageRange directly.  
--- Always use write_record() and read_record().