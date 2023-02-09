use num_derive::FromPrimitive;

#[cfg(test)]
mod tests;

struct Container {
    version: &'static str,
    family: ContainerFamily,
    class: ContainerClass,
    ctype: &'static u8,
    uuid: &'static str,
    allocated_components: &'static u8,
    used_components: &'static u8,
    dim1: &'static i32,
    dim2: &'static i32,
    base1_uuid: &'static str,
    base2_uuid: &'static str,
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
