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

struct BOMEntry {
    u8 family;
    u8 type;
    u8 mode;
    char name[13];
    s64 offset;
    s64 size;
    s64 param1;
    s64 param2;
};

ContainerHeader header @ 0x00;
BOMEntry bom[header.allocated_components] @ 0xA0;
