use num_derive::FromPrimitive;

#[cfg(test)]
mod tests;

struct Container<'a> {
    version: &'a str,
    family: ContainerFamily,
    class: ContainerClass,
    ctype: &'a u8,
    uuid: &'a str,
    allocated_components: &'a u8,
    used_components: &'a u8,
    dim1: &'a i32,
    dim2: &'a i32,
    base1_uuid: &'a str,
    base2_uuid: &'a str,
    components: Vec<Component>, 
}

#[repr(u8)]
#[derive(FromPrimitive)]
enum ContainerFamily {
    Ziggurat = 0x5A,
    ApplicationDefined = 0x41,
}

#[repr(u8)]
#[derive(FromPrimitive)]
enum ContainerClass {
    Layer = 0x4C,
    Variable = 0x56,
    Ephemera = 0x45,
}

enum Component {

}
