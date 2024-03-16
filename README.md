# rust_redis
練習rust的項目


# redis notes

in this project we only implemented

bulk string
simple string
simple error
array
integer 


# - Simple strings
 For example, many Redis commands reply with just "OK" on success. The encoding of this Simple String is the following 5 bytes:
```
        +Ok\r\n
        we call \r\n a CRLF bytes
```
# - Simple Error

RESP has specific data types for errors. Simple errors, or simply just errors, are similar to simple strings, but their first character is the minus (-) character. The difference between simple strings and errors in RESP is that clients should treat errors as exceptions, whereas the string encoded in the error type is the error message itself.

```
        -Error message\r\n
```

Redis replies with an error only when something goes wrong, for example, when you try to operate against the wrong data type, or when the command does not exist. The client should raise an exception when it receives an Error reply.
    
```
    -ERR unknown command 'asdf'
    -WRONGTYPE Operation against a key holding the wrong kind of value
```

# - Integers 

this type is a CRLF-terminated string that represents a signed, base-10, 64-bit integer
it encodes like the following ways
```
:[<+|->]<value>\r\n
```

some explaination
- The colon (:) as the first byte.
- An optional plus (+) or minus (-) as the sign.
- One or more decimal digits (0..9) as the integer's unsigned, base-10 value.
- The CRLF terminator.

e.g 
```
:0\r\n
:1000\r\n
```

# - Bulk strings 

A bulk string represents a single binary string. The string can be of any size, but by default, Redis limits it to 512 MB (see the proto-max-bulk-len configuration directive).

```
$<length>\r\n<data>\r\n
```
explaination:

- The dollar sign ($) as the first byte
- one or more decimal digits (0..9) as the string length, in bytes, as an unsigned, base-10 value

- The CRLF terminator
- The data
- A final CRLF

the string hello would be encoded as follow
```
$5\r\nhello\r\n
```
The empty string's encoding is:
```
$0\r\n\r\n
```

# - Arrays

Clients send commands to the Redis server as RESP arrays. Similarly, some Redis commands that return collections of elements use arrays as their replies. An example is the LRANGE command that returns elements of a list.

```
*<number-of-elements>\r\n<element-1>...<element-n>
```

- An asterisk (*) as the first byte.
- One or more decimal digits (0..9) as the number of elements in the array as an - unsigned, base-10 value.
- The CRLF terminator.
- An additional RESP type for every element of the array.

hello world would be encoded like this

```
*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n=
```