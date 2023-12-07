#pragma endian little

struct ContainerHeader {
    char magic[8];
    char version[3];
    char family;
    char class;
    char type;
    u8 allocated_components;
    u8 used_components;
    be u128 uuid;
    be u128 base1_uuid;
    be u128 base2_uuid;

    s64 dim1;
    s64 dim2;
    s64 extensions;
    
    char comment[72];
};

struct String {
    char string[];
} [[inline]];

struct VarInt {
    u8 b1 ;
    if (b1 & 0x80) {
        u8 b2;
        if (b2 & 0x80) {
            u8 b3;
            if (b3 & 0x80) {
                u8 b4;
                if (b4 & 0x80) {
                    u8 b5;
                    if (b5 & 0x80) {
                        u8 b6;
                        if (b6 & 0x80) {
                            u8 b7;
                            if (b7 & 0x80) {
                                u8 b8;
                                if (b8 & 0x80) {
                                    u8 b9;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
} [[single_color]];

struct DeltaBlock {
    VarInt delta_block[16];
} [[inline]];

struct BOMEntry {
    u8 family;
    u8 type;
    u8 mode;
    char name[13];
    s64 offset;
    s64 size;
    s64 param1;
    s64 param2;
    
    if (type == 0x02 && mode == 0x00) { // StringList
        //String string[param1] @ offset;
        u8 strings[size] @ offset;
    } else if (type == 0x03 && mode == 0x00) { // StringVector
        s64 offsets[param1+1] @ offset;
        u8 strings[size - ((param1+1)*8)] @ offset + ((param1+1)*8);
    } else if (type == 0x04 && mode == 0x00) { // Vector
        s64 data[param1*param2] @ offset;
    } else if (type == 0x04 && mode == 0x02) { // Vector:delta
        s64 sync[m(param1)] @ offset;
        //DeltaBlock data[m(param1)] @ offset + (8*m(param1));
        u8 data[size - (m(param1)*8)] @ offset + m(param1)*8;
    } else if (type == 0x06 && mode == 0x00) { // Index
        s64 pairs[param1*2] @ offset;
    } else if (type == 0x06 && mode == 0x01) { // Index:comp
        s64 r @ offset;
        s64 sync[m(r) * 2] @ offset + 8;
        u8 data[size-(m(r)*16+8)] @ offset + 8 + (m(r)*16);
    } else if (type == 0x07 && mode == 0x01) { // InvertedIdx
        s64 typeinfo[param1*2] @ offset;
        u8 data[size - (param1*16)] @ offset + (param1*16);
    }
};

fn m(s64 size) {
    return ((size-1)/16)+1;
};

ContainerHeader header @ 0x00;
BOMEntry bom[header.allocated_components] @ 0xA0;

