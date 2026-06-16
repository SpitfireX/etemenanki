use crate::Query;

#[test]
fn test_q1() {
    let q1 = r#""hello" "world" | "hi" [pos="NN"]{,2} | "yo" ("world" | "planet")"#;
    let r = Query::parse(&q1).unwrap();
    println!("{:#?}", r);
}

#[test]
fn test_q2() {
    let q2 = r#"( [pos = "DT"]? [pos = "JJ.*"]* [pos = "NNS?"] | [pos = "NPS?"]+ | [pos = "PP"] ) [!lemma = "say" & !( pos = "V.*" | bla = "arst") & fr = "frfr"]"#;
    let r = Query::parse(q2).unwrap();
    println!("{:#?}", r);
}

#[test]
fn test_q3() {
    let q3 = r#"[pos="test" & !(lemma="blabla" | !bla="blaaa" | !(foo="bar" & bar="baz"))]"#;
    let r = Query::parse(q3).unwrap();
    println!("{:#?}", r);
}

#[test]
fn test_q4() {
    let q4 = r#"[pos="DT"] ([] [])* [pos="N.+"]+"#;
    let r = Query::parse(q4).unwrap();
    println!("{:#?}", r);
}
