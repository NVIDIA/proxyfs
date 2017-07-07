#ifndef __PFS_BASE64_H__
#define __PFS_BASE64_H__

// Base64-related encode and decode functions
char *encode_binary(const uint8_t *inbuf, size_t inbuf_size);
void decode_binary(const char *inbuf, uint8_t *outbuf, size_t outbuf_size, size_t *bytes_written);


#endif
