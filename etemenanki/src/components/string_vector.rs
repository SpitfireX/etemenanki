use std::{
    ops,
    str::pattern::{Pattern, ReverseSearcher},
};

#[derive(Debug, Clone, Copy)]
pub struct StringVector<'map> {
    length: usize,
    offsets: &'map [i64],
    data: &'map [u8],
}

impl<'map> StringVector<'map> {
    pub fn all_containing<'a: 'map, P>(&'a self, pat: P) -> PatternIterator<'map, 'a, P>
    where
        P: Pattern<'a> + Copy,
        <P as Pattern<'a>>::Searcher: ReverseSearcher<'a>,
    {
        PatternIterator {
            strvec: self,
            index: 0,
            pattern: pat,
            fun: str::contains,
        }
    }

    pub fn all_ending_with<'a: 'map, P>(&'a self, pat: P) -> PatternIterator<'map, 'a, P>
    where
        P: Pattern<'a> + Copy,
        <P as Pattern<'a>>::Searcher: ReverseSearcher<'a>,
    {
        PatternIterator {
            strvec: self,
            index: 0,
            pattern: pat,
            fun: str::ends_with,
        }
    }

    pub fn all_starting_with<'a: 'map, P>(&'a self, pat: P) -> PatternIterator<'map, 'a, P>
    where
        P: Pattern<'a> + Copy,
        <P as Pattern<'a>>::Searcher: ReverseSearcher<'a>,
    {
        PatternIterator {
            strvec: self,
            index: 0,
            pattern: pat,
            fun: str::starts_with,
        }
    }

    pub fn from_parts(n: usize, offsets: &'map [i64], data: &'map [u8]) -> Self {
        assert!(n + 1 == offsets.len());
        Self {
            length: n,
            offsets,
            data,
        }
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        if index < self.len() {
            Some(&self.get_unchecked(index))
        } else {
            None
        }
    }

    pub fn get_unchecked(&self, index: usize) -> &str {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        unsafe { std::str::from_utf8_unchecked(&self.data[start..end - 1]) }
    }

    pub fn get_all<'a: 'map, I>(&'a self, indices: I) -> impl Iterator<Item = &'map str>
    where
        I: IntoIterator<Item = &'a usize>,
    {
        indices.into_iter().map(|x| &self[*x])
    }

    pub fn iter(&self) -> StringVectorIterator {
        self.into_iter()
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

impl<'map> ops::Index<usize> for StringVector<'map> {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        &self.get_unchecked(index)
    }
}

pub struct StringVectorIterator<'map> {
    vec: &'map StringVector<'map>,
    index: usize,
}

impl<'map> Iterator for StringVectorIterator<'map> {
    type Item = &'map str;

    fn next(&mut self) -> Option<Self::Item> {
        match self.vec.get(self.index) {
            Some(str) => {
                self.index += 1;
                Some(str)
            }
            None => None,
        }
    }
}

impl<'map> IntoIterator for &'map StringVector<'map> {
    type Item = &'map str;
    type IntoIter = StringVectorIterator<'map>;

    fn into_iter(self) -> Self::IntoIter {
        StringVectorIterator {
            vec: self,
            index: 0,
        }
    }
}

pub struct PatternIterator<'map, 'a, P>
where
    P: Pattern<'a> + Copy,
    <P as Pattern<'a>>::Searcher: ReverseSearcher<'a>,
{
    strvec: &'a StringVector<'map>,
    index: usize,
    pattern: P,
    fun: fn(&'a str, P) -> bool,
}

impl<'map: 'a, 'a, P> PatternIterator<'map, 'a, P>
where
    P: Pattern<'a> + Copy,
    <P as Pattern<'a>>::Searcher: ReverseSearcher<'a>,
{
    pub fn as_strs(self) -> impl Iterator<Item = &'a str> {
        let strvec = self.strvec;
        self.map(|i| &strvec[i])
    }

    pub fn collect_strs<B>(self) -> B 
    where
        B: FromIterator<&'a str>,
    {
        self.as_strs().collect()
    }
}

impl<'map: 'a, 'a, P> Iterator for PatternIterator<'map, 'a, P>
where
    P: Pattern<'a> + Copy,
    <P as Pattern<'a>>::Searcher: ReverseSearcher<'a>,
{
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.strvec.len() {
            let current = &self.strvec[self.index];
            self.index += 1;

            if !(self.fun)(current, self.pattern) {
                continue;
            } else {
                return Some(self.index - 1);
            }
        }
        None
    }
}
