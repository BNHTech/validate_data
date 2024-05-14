# Validate Data (row-by-row)

## Requirements

- python3.9
- pip3

## Install Dependencies

```bash 
pip3 install -r req.txt 
```

## Validate Data

### Update connection strings

update `src_connection_str` and `dest_connection_str` in `validate.py`

### Execute validator
```bash 
export TABLE_NAME=""

python3 "${TABLE_NAME}"
```