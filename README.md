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

## Test command
for mysql 
```mysql
CREATE TABLE TEST_GO.EMOJI_TABLE (
    ID INT,
    EMOJI_COLUMN LONGTEXT
);
ALTER TABLE TEST_GO.EMOJI_TABLE ADD PRIMARY KEY(id);

update TEST_GO.EMOJI_TABLE set EMOJI_COLUMN = from_base64('VGVzdCDwn5iC') where id = 123;
```
