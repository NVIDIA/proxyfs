#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <stdint.h>
#include "debug.h"


// Base64 encode/decode
const char base64_encode_table[] = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
    'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
};

char *encode_binary(const uint8_t *inbuf, size_t inbuf_size)
{
    int       inbuf_pos;
    uint32_t  inbuf_uint24;
    char     *outbuf;
    size_t    outbuf_len;
    int       outbuf_pos;
    int       trailing_equals_count;

    outbuf_len = (inbuf_size + 2) / 3;
    outbuf_len = outbuf_len * 4;

    trailing_equals_count = inbuf_size % 3;
    if (0 != trailing_equals_count) {
        trailing_equals_count = 3 - trailing_equals_count;
    }

    outbuf = (char *)malloc(outbuf_len + 1);

    if ((char *)0 != outbuf) {
        for (outbuf_pos = 0; outbuf_pos < outbuf_len; outbuf_pos += 4) {
            inbuf_pos = outbuf_pos / 4;
            inbuf_pos = inbuf_pos  * 3;

            inbuf_uint24 = (inbuf[inbuf_pos] << 16) + (inbuf[inbuf_pos + 1] << 8) + inbuf[inbuf_pos + 2];

            outbuf[outbuf_pos + 0] = base64_encode_table[(inbuf_uint24 >> 18) & 0b111111];
            outbuf[outbuf_pos + 1] = base64_encode_table[(inbuf_uint24 >> 12) & 0b111111];
            outbuf[outbuf_pos + 2] = base64_encode_table[(inbuf_uint24 >>  6) & 0b111111];
            outbuf[outbuf_pos + 3] = base64_encode_table[(inbuf_uint24 >>  0) & 0b111111];
        }

        if (2 == trailing_equals_count) {
            inbuf_uint24 = inbuf[inbuf_size - 1] << 16;

            outbuf[outbuf_len - 4] = base64_encode_table[(inbuf_uint24 >> 18) & 0b111111];
            outbuf[outbuf_len - 3] = base64_encode_table[(inbuf_uint24 >> 12) & 0b111111];
            outbuf[outbuf_len - 2] = '=';
            outbuf[outbuf_len - 1] = '=';
         }

        if (1 == trailing_equals_count) {
            inbuf_uint24 = (inbuf[inbuf_size - 2] << 16) + (inbuf[inbuf_size - 1] << 8);

            outbuf[outbuf_len - 4] = base64_encode_table[(inbuf_uint24 >> 18) & 0b111111];
            outbuf[outbuf_len - 3] = base64_encode_table[(inbuf_uint24 >> 12) & 0b111111];
            outbuf[outbuf_len - 2] = base64_encode_table[(inbuf_uint24 >>  6) & 0b111111];
            outbuf[outbuf_len - 1] = '=';
        }

        outbuf[outbuf_len] = '\0';
    }

    return outbuf;
}

const uint8_t base64_decode_table[] = {
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // NUL, SOH, STX, ETX, EOT, ENQ, ACK, BEL,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // BS , TAB, LF , VT , FF , CR , SO , SI ,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // DLE, DC1, DC2, DC3, DC4, NAK, SYN, ETB,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // CAN, EM , SUB, ESC, FS , GS , RS , US ,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ' ', !  , "  , #  , $  , %  , &  , '  ,
    0b11000000, 0b11000000, 0b11000000, 0b00111110, 0b11000000, 0b11000000, 0b11000000, 0b00111111, // (  , )  , *  , +  , ',', -  , .  , /  ,
    0b00110100, 0b00110101, 0b00110110, 0b00110111, 0b00111000, 0b00111001, 0b00111010, 0b00111011, // 0  , 1  , 2  , 3  , 4  , 5  , 6  , 7  ,
    0b00111100, 0b00111101, 0b11000000, 0b11000000, 0b11000000, 0b10000000, 0b11000000, 0b11000000, // 8  , 9  , :  , ;  , <  , =  , >  , ?  ,
    0b11000000, 0b00000000, 0b00000001, 0b00000010, 0b00000011, 0b00000100, 0b00000101, 0b00000110, // @  , A  , B  , C  , D  , E  , F  , G  ,
    0b00000111, 0b00001000, 0b00001001, 0b00001010, 0b00001011, 0b00001100, 0b00001101, 0b00001110, // H  , I  , J  , K  , L  , M  , N  , O  ,
    0b00001111, 0b00010000, 0b00010001, 0b00010010, 0b00010011, 0b00010100, 0b00010101, 0b00010110, // P  , Q  , R  , S  , T  , U  , V  , W  ,
    0b00010111, 0b00011000, 0b00011001, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // X  , Y  , Z  , [  , \  , ]  , ^  , _  ,
    0b11000000, 0b00011010, 0b00011011, 0b00011100, 0b00011101, 0b00011110, 0b00011111, 0b00100000, // `  , a  , b  , c  , d  , e  , f  , g  ,
    0b00100001, 0b00100010, 0b00100011, 0b00100100, 0b00100101, 0b00100110, 0b00100111, 0b00101000, // h  , i  , j  , k  , l  , m  , n  , o  ,
    0b00101001, 0b00101010, 0b00101011, 0b00101100, 0b00101101, 0b00101110, 0b00101111, 0b00110000, // p  , q  , r  , s  , t  , u  , v  , w  ,
    0b00110001, 0b00110010, 0b00110011, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // x  , y  , z  , {  , |  , }  , ~  , ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, // ---, ---, ---, ---, ---, ---, ---, ---,
    0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000, 0b11000000  // ---, ---, ---, ---, ---, ---, ---, ---
};

void decode_binary(const char *inbuf, uint8_t *outbuf, size_t outbuf_size, size_t *bytes_written)
{
    int      inbuf_len;
    int      inbuf_pos;
    uint32_t inbuf_uint24;
    int      outbuf_pos;

    inbuf_len = strlen(inbuf);

    inbuf_pos  = 0;
    outbuf_pos = 0;

    while (1) {
        outbuf_pos = inbuf_pos  / 4;
        outbuf_pos = outbuf_pos * 3;

        if ('\0' == inbuf[inbuf_pos]) {
            *bytes_written = (size_t)(outbuf_pos + 0);

            return;
        }

        inbuf_uint24 = base64_decode_table[inbuf[inbuf_pos + 0]] & 0b111111;
        inbuf_uint24 = (inbuf_uint24 << 6) + (base64_decode_table[inbuf[inbuf_pos + 1]] & 0b111111);
        inbuf_uint24 = (inbuf_uint24 << 6) + (base64_decode_table[inbuf[inbuf_pos + 2]] & 0b111111);
        inbuf_uint24 = (inbuf_uint24 << 6) + (base64_decode_table[inbuf[inbuf_pos + 3]] & 0b111111);
 
        if ('=' == inbuf[inbuf_pos + 2]) {
            outbuf[outbuf_pos + 0] = inbuf_uint24 >> 16 & 0b11111111;

            *bytes_written = (size_t)(outbuf_pos + 1);

            return;
        }

        if ('=' == inbuf[inbuf_pos + 3]) {
            outbuf[outbuf_pos + 0] = inbuf_uint24 >> 16 & 0b11111111;
            outbuf[outbuf_pos + 1] = inbuf_uint24 >>  8 & 0b11111111;

            *bytes_written = (size_t)(outbuf_pos + 2);

            return;
        }

        outbuf[outbuf_pos + 0] = inbuf_uint24 >> 16 & 0b11111111;
        outbuf[outbuf_pos + 1] = inbuf_uint24 >>  8 & 0b11111111;
        outbuf[outbuf_pos + 2] = inbuf_uint24 >>  0 & 0b11111111;

        inbuf_pos += 4;
    }
}

